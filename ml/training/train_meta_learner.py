"""
Stacking Meta-Learner: Combines 3 calibrated component scores into a single final risk probability.
"""

from sklearn.linear_model import LogisticRegression
import numpy as np
import joblib


class StackingMetaLearner:
    def __init__(self):
        self.meta_model = LogisticRegression(
            class_weight="balanced",
            C=0.1,
            solver="lbfgs",
            random_state=42
        )

    def fit(
        self,
        cal_p_xgb: np.ndarray,
        cal_p_ae: np.ndarray,
        cal_p_if: np.ndarray,
        y_val: np.ndarray
    ):
        meta_features = np.column_stack([cal_p_xgb, cal_p_ae, cal_p_if])
        self.meta_model.fit(meta_features, y_val)
        print(f"Meta-Learner Coefficients: XGB={self.meta_model.coef_[0][0]:.4f}, "
              f"AE={self.meta_model.coef_[0][1]:.4f}, IF={self.meta_model.coef_[0][2]:.4f}")
        return self

    def predict_proba(
        self,
        cal_p_xgb: np.ndarray,
        cal_p_ae: np.ndarray,
        cal_p_if: np.ndarray
    ) -> np.ndarray:
        meta_features = np.column_stack([cal_p_xgb, cal_p_ae, cal_p_if])
        return self.meta_model.predict_proba(meta_features)[:, 1]

    def save(self, path: str):
        joblib.dump(self, path)

    @staticmethod
    def load(path: str):
        return joblib.load(path)
