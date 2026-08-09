"""
End-to-End Training Pipeline Orchestration.
"""

import mlflow
import numpy as np
import joblib
import json
import os
import torch
from sklearn.metrics import average_precision_score

from fraud_detection.ml.training.train_supervised import train_xgboost_supervised
from fraud_detection.ml.training.models.autoencoder import train_autoencoder
from fraud_detection.ml.training.train_isolation_forest import train_isolation_forest, get_isolation_scores
from fraud_detection.ml.training.calibrate_model import ScoreCalibrator
from fraud_detection.ml.training.train_meta_learner import StackingMetaLearner
from fraud_detection.ml.training.data_preparation import prepare_training_dataset
from fraud_detection.ml.ensemble.ensemble_model import FraudEnsemblePyFunc

MIN_PR_AUC_GATE = 0.80


def run_training_pipeline(spark):
    mlflow.set_experiment("/Shared/fraud_detection_training")

    with mlflow.start_run(run_name="ensemble_training_v1") as run:
        train_df, val_df, test_df = prepare_training_dataset(spark)
        feature_cols = [c for c in train_df.columns if c.startswith(("feature_", "vel_", "geo_", "base_", "merch_", "graph_"))]

        X_train = train_df.select(feature_cols).toPandas().values
        y_train = train_df.select("is_fraud_reconciled").toPandas().values.ravel()
        X_val = val_df.select(feature_cols).toPandas().values
        y_val = val_df.select("is_fraud_reconciled").toPandas().values.ravel()
        X_test = test_df.select(feature_cols).toPandas().values
        y_test = test_df.select("is_fraud_reconciled").toPandas().values.ravel()

        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("train_size", len(y_train))

        xgb_model = train_xgboost_supervised(X_train, y_train, X_val, y_val, feature_cols)
        raw_p_xgb_val = xgb_model.predict_proba(X_val)[:, 1]

        X_legit_train = X_train[y_train == 0]
        ae_model, ae_scaler = train_autoencoder(X_legit_train, input_dim=len(feature_cols))
        scaled_val = ae_scaler.transform(X_val)
        raw_err_ae_val = ae_model.reconstruction_error(torch.tensor(scaled_val, dtype=torch.float32))

        iso_model = train_isolation_forest(X_legit_train)
        raw_score_if_val = get_isolation_scores(iso_model, X_val)

        cal_xgb = ScoreCalibrator("isotonic").fit(raw_p_xgb_val, y_val)
        cal_ae = ScoreCalibrator("isotonic").fit(raw_err_ae_val, y_val)
        cal_if = ScoreCalibrator("isotonic").fit(raw_score_if_val, y_val)

        meta_learner = StackingMetaLearner()
        meta_learner.fit(
            cal_xgb.transform(raw_p_xgb_val),
            cal_ae.transform(raw_err_ae_val),
            cal_if.transform(raw_score_if_val),
            y_val
        )

        raw_p_xgb_test = xgb_model.predict_proba(X_test)[:, 1]
        scaled_test = ae_scaler.transform(X_test)
        raw_err_ae_test = ae_model.reconstruction_error(torch.tensor(scaled_test, dtype=torch.float32))
        raw_score_if_test = get_isolation_scores(iso_model, X_test)

        final_probs = meta_learner.predict_proba(
            cal_xgb.transform(raw_p_xgb_test),
            cal_ae.transform(raw_err_ae_test),
            cal_if.transform(raw_score_if_test)
        )

        test_pr_auc = average_precision_score(y_test, final_probs)
        mlflow.log_metric("test_pr_auc", test_pr_auc)

        if test_pr_auc >= MIN_PR_AUC_GATE:
            print(f"✅ Quality Gate PASSED (PR-AUC {test_pr_auc:.4f} >= {MIN_PR_AUC_GATE}). Registering model...")
            artifacts_dir = "/tmp/ensemble_artifacts"
            os.makedirs(artifacts_dir, exist_ok=True)
            joblib.dump(xgb_model, f"{artifacts_dir}/xgb_model.pkl")
            torch.save(ae_model, f"{artifacts_dir}/ae_model.pt")
            joblib.dump(ae_scaler, f"{artifacts_dir}/ae_scaler.pkl")
            joblib.dump(iso_model, f"{artifacts_dir}/iso_forest.pkl")
            joblib.dump(cal_xgb, f"{artifacts_dir}/cal_xgb.pkl")
            joblib.dump(cal_ae, f"{artifacts_dir}/cal_ae.pkl")
            joblib.dump(cal_if, f"{artifacts_dir}/cal_if.pkl")
            joblib.dump(meta_learner, f"{artifacts_dir}/meta_learner.pkl")
            with open(f"{artifacts_dir}/feature_names.json", "w") as f:
                json.dump(feature_cols, f)
            with open(f"{artifacts_dir}/model_version.txt", "w") as f:
                f.write(run.info.run_id)

            mlflow.pyfunc.log_model(
                artifact_path="ensemble_model",
                python_model=FraudEnsemblePyFunc(),
                registered_model_name="fraud-ensemble-champion",
                artifacts={
                    "xgb_model": f"{artifacts_dir}/xgb_model.pkl",
                    "ae_model": f"{artifacts_dir}/ae_model.pt",
                    "ae_scaler": f"{artifacts_dir}/ae_scaler.pkl",
                    "iso_forest": f"{artifacts_dir}/iso_forest.pkl",
                    "cal_xgb": f"{artifacts_dir}/cal_xgb.pkl",
                    "cal_ae": f"{artifacts_dir}/cal_ae.pkl",
                    "cal_if": f"{artifacts_dir}/cal_if.pkl",
                    "meta_learner": f"{artifacts_dir}/meta_learner.pkl",
                    "feature_names": f"{artifacts_dir}/feature_names.json",
                    "model_version_tag": f"{artifacts_dir}/model_version.txt"
                }
            )
        else:
            print(f"❌ Quality Gate FAILED (PR-AUC {test_pr_auc:.4f} < {MIN_PR_AUC_GATE}). Model NOT registered.")
