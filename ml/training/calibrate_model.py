"""
Score Calibrator: Maps raw component model outputs to calibrated probabilities.
Uses Isotonic Regression or Sigmoid Platt Scaling.
"""

from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
import numpy as np
import joblib


class ScoreCalibrator:
    def __init__(self, method: str = "isotonic"):
        self.method = method
        if method == "isotonic":
            self.calibrator = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
        else:
            self.calibrator = LogisticRegression(C=1.0)

    def fit(self, raw_scores: np.ndarray, y_val: np.ndarray):
        raw_scores = raw_scores.ravel()
        if self.method == "isotonic":
            self.calibrator.fit(raw_scores, y_val)
        else:
            self.calibrator.fit(raw_scores.reshape(-1, 1), y_val)
        return self

    def transform(self, raw_scores: np.ndarray) -> np.ndarray:
        raw_scores = raw_scores.ravel()
        if self.method == "isotonic":
            return self.calibrator.transform(raw_scores)
        else:
            return self.calibrator.predict_proba(raw_scores.reshape(-1, 1))[:, 1]

    def save(self, path: str):
        joblib.dump(self, path)

    @staticmethod
    def load(path: str):
        return joblib.load(path)
