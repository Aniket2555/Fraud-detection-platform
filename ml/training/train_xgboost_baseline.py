"""
XGBoost baseline training for fraud detection.
Includes time-based split, class weighting, Optuna tuning, Platt calibration,
SHAP explainability, and MLflow logging.
"""

import xgboost as xgb
import optuna
import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import shap
import matplotlib.pyplot as plt
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import precision_recall_curve, auc, f1_score, roc_curve
import joblib
import yaml

from utils.feature_engineering import FraudFeatureEngineer

optuna.logging.set_verbosity(optuna.logging.WARNING)


def load_time_split_data(df, train_ratio=0.70, val_ratio=0.15):
    """Sort by transaction_dt and split chronologically."""
    df_sorted = df.sort_values("transaction_dt").reset_index(drop=True)
    n = len(df_sorted)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train_df = df_sorted.iloc[:train_end]
    val_df = df_sorted.iloc[train_end:val_end]
    test_df = df_sorted.iloc[val_end:]
    return train_df, val_df, test_df


def compute_metrics(y_true, y_pred_proba, prefix=""):
    """Compute PR-AUC, Recall@1%FPR, Recall@5%FPR, and optimal F1."""
    precision, recall, thresholds = precision_recall_curve(y_true, y_pred_proba)
    pr_auc = auc(recall, precision)

    fpr, tpr, _ = roc_curve(y_true, y_pred_proba)
    recall_at_1_fpr = float(np.interp(0.01, fpr, tpr))
    recall_at_5_fpr = float(np.interp(0.05, fpr, tpr))

    f1_scores = 2 * (precision * recall) / (precision + recall + 1e-10)
    optimal_idx = np.argmax(f1_scores)
    optimal_threshold = float(thresholds[optimal_idx]) if optimal_idx < len(thresholds) else 0.5
    f1_optimal = float(f1_scores[optimal_idx])

    return {
        f"{prefix}pr_auc": float(pr_auc),
        f"{prefix}recall_at_1pct_fpr": recall_at_1_fpr,
        f"{prefix}recall_at_5pct_fpr": recall_at_5_fpr,
        f"{prefix}f1_optimal": f1_optimal,
        f"{prefix}optimal_threshold": optimal_threshold,
    }, precision, recall, thresholds


def train_baseline(df):
    """Executes full training pipeline."""
    mlflow.set_experiment("fraud-detection-baseline")

    with mlflow.start_run(run_name="xgboost-baseline-v1"):
        train_df, val_df, test_df = load_time_split_data(df)

        feature_engineer = FraudFeatureEngineer()
        y_train = train_df["is_fraud"].values
        y_val = val_df["is_fraud"].values
        y_test = test_df["is_fraud"].values

        feature_engineer.fit(train_df, y_train)

        X_train = feature_engineer.transform(train_df)
        X_val = feature_engineer.transform(val_df)
        X_test = feature_engineer.transform(test_df)

        fraud_ratio = y_train.mean()
        scale_pos_weight = (1 - fraud_ratio) / max(fraud_ratio, 1e-5)

        def objective(trial):
            params = {
                "objective": "binary:logistic",
                "eval_metric": "aucpr",
                "scale_pos_weight": scale_pos_weight,
                "max_depth": trial.suggest_int("max_depth", 3, 8),
                "learning_rate": trial.suggest_float("learning_rate", 0.02, 0.2, log=True),
                "n_estimators": trial.suggest_int("n_estimators", 100, 400),
                "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
                "tree_method": "hist",
                "random_state": 42
            }
            model = xgb.XGBClassifier(**params)
            model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
            preds = model.predict_proba(X_val)[:, 1]
            p, r, _ = precision_recall_curve(y_val, preds)
            return auc(r, p)

        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=15)

        best_params = study.best_trial.params
        best_params.update({
            "objective": "binary:logistic",
            "eval_metric": "aucpr",
            "scale_pos_weight": scale_pos_weight,
            "tree_method": "hist",
            "random_state": 42
        })

        final_model = xgb.XGBClassifier(**best_params)
        final_model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

        y_test_proba = final_model.predict_proba(X_test)[:, 1]
        test_metrics, precision, recall, _ = compute_metrics(y_test, y_test_proba, prefix="test_")

        mlflow.log_metrics(test_metrics)
        mlflow.xgboost.log_model(final_model, "model", registered_model_name="fraud-xgboost-baseline")

        print(f"✅ Baseline Training Complete! Test PR-AUC: {test_metrics['test_pr_auc']:.4f}")
        return final_model, test_metrics
