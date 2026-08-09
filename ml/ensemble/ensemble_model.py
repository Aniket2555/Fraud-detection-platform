"""
MLflow Custom PyFunc Wrapper for the Fraud Detection Hybrid Ensemble.

Production fixes applied:
- Input validation: shape check, NaN/Inf guard, feature alignment check
- predict_df() overload for Azure ML online endpoint DataFrame input compatibility
- model_version loaded from context artifacts (not hardcoded)
- Graceful handling of single-row vs batch inputs
- Explicit torch.no_grad() context for reconstruction_error calls
"""

import json
import logging

import mlflow.pyfunc
import numpy as np
import torch

logger = logging.getLogger("fraud_ensemble")


class FraudEnsemblePyFunc(mlflow.pyfunc.PythonModel):
    """
    Hybrid ensemble wrapper combining XGBoost, Autoencoder, and Isolation Forest.
    Accepts raw numpy arrays or pandas DataFrames.
    """

    def load_context(self, context):
        """Loads all model artifacts from MLflow storage context."""
        import joblib

        self.xgb_model = joblib.load(context.artifacts["xgb_model"])
        self.ae_model = torch.load(context.artifacts["ae_model"], weights_only=False)
        self.ae_model.eval()
        self.ae_scaler = joblib.load(context.artifacts["ae_scaler"])
        self.iso_forest = joblib.load(context.artifacts["iso_forest"])
        self.cal_xgb = joblib.load(context.artifacts["cal_xgb"])
        self.cal_ae = joblib.load(context.artifacts["cal_ae"])
        self.cal_if = joblib.load(context.artifacts["cal_if"])
        self.meta_learner = joblib.load(context.artifacts["meta_learner"])

        with open(context.artifacts["feature_names"], "r") as f:
            self.feature_names: list = json.load(f)

        # Load model_version from artifact metadata if available
        try:
            with open(context.artifacts.get("model_version_tag", ""), "r") as fv:
                self.model_version = fv.read().strip()
        except Exception:
            self.model_version = "unknown"

        logger.info("FraudEnsemblePyFunc loaded. features=%d version=%s",
                    len(self.feature_names), self.model_version)

    def _validate_and_coerce(self, model_input) -> np.ndarray:
        """
        Validates and coerces model_input to a float32 numpy array.
        Accepts np.ndarray, pd.DataFrame, or list.
        Raises ValueError for irrecoverable shape mismatches.
        """
        import pandas as pd

        if isinstance(model_input, pd.DataFrame):
            # Align columns to expected feature order; fill missing with 0
            aligned = model_input.reindex(columns=self.feature_names, fill_value=0.0)
            arr = aligned.values.astype(np.float32)
        elif isinstance(model_input, np.ndarray):
            arr = model_input.astype(np.float32)
        elif isinstance(model_input, list):
            arr = np.array(model_input, dtype=np.float32)
        else:
            raise TypeError(f"Unsupported model_input type: {type(model_input)}")

        # Ensure 2-D
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)

        # Feature count check
        if arr.shape[1] != len(self.feature_names):
            raise ValueError(
                f"Feature count mismatch: expected {len(self.feature_names)}, got {arr.shape[1]}. "
                "Ensure the feature vector aligns with the training schema."
            )

        # NaN / Inf guard
        if not np.all(np.isfinite(arr)):
            logger.warning("Non-finite values in model input — replacing with 0.0.")
            arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)

        return arr

    def _score_array(self, arr: np.ndarray) -> dict:
        """Core scoring logic for a validated float32 numpy array (batch or single row)."""
        # XGBoost
        raw_p_xgb = self.xgb_model.predict_proba(arr)[:, 1]

        # Autoencoder — explicit no_grad to prevent gradient accumulation
        scaled_input = self.ae_scaler.transform(arr)
        tensor_input = torch.tensor(scaled_input, dtype=torch.float32)
        with torch.no_grad():
            raw_err_ae = self.ae_model.reconstruction_error(tensor_input)

        # Isolation Forest
        raw_score_if = -self.iso_forest.decision_function(arr)

        # Isotonic calibration
        cal_p_xgb = self.cal_xgb.transform(raw_p_xgb)
        cal_p_ae = self.cal_ae.transform(raw_err_ae)
        cal_p_if = self.cal_if.transform(raw_score_if)

        # Stacking meta-learner
        final_fraud_prob = self.meta_learner.predict_proba(cal_p_xgb, cal_p_ae, cal_p_if)

        return {
            "fraud_probability": float(final_fraud_prob[0]),
            "component_scores": {
                "xgboost": float(cal_p_xgb[0]),
                "autoencoder": float(cal_p_ae[0]),
                "isolation_forest": float(cal_p_if[0]),
            },
            "scoring_mode": "full",
            "model_version": self.model_version,
        }

    def predict(self, context, model_input) -> dict:
        """
        MLflow PyFunc predict entry point.
        Accepts np.ndarray, pd.DataFrame, or list.
        Returns a dict with fraud_probability, component_scores, scoring_mode.
        """
        arr = self._validate_and_coerce(model_input)
        return self._score_array(arr)

    def predict_df(self, df) -> "pd.DataFrame":
        """
        Azure ML managed online endpoint compatible batch predict.
        Accepts a pandas DataFrame, returns a DataFrame with scores appended.
        """
        import pandas as pd

        arr = self._validate_and_coerce(df)
        result = self._score_array(arr)

        out = df.copy()
        out["fraud_probability"] = result["fraud_probability"]
        out["xgboost_score"] = result["component_scores"]["xgboost"]
        out["autoencoder_score"] = result["component_scores"]["autoencoder"]
        out["isolation_forest_score"] = result["component_scores"]["isolation_forest"]
        out["model_version"] = result["model_version"]
        return out
