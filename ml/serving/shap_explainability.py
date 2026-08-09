"""
SHAP TreeExplainer for Supervised Model Interpretability.
Generates top-K contributing risk features per prediction.
"""

import shap
import numpy as np


class FraudExplainer:
    def __init__(self, model, feature_names: list):
        self.explainer = shap.TreeExplainer(model)
        self.feature_names = feature_names

    def get_top_k_reasons(self, feature_vector: np.ndarray, top_k: int = 5) -> list:
        if feature_vector.ndim == 1:
            feature_vector = feature_vector.reshape(1, -1)

        shap_values = self.explainer.shap_values(feature_vector)

        if isinstance(shap_values, list):
            sv = shap_values[1][0]
        else:
            sv = shap_values[0]

        top_indices = np.argsort(sv)[::-1][:top_k]

        reasons = []
        for idx in top_indices:
            if sv[idx] > 0:
                reasons.append({
                    "feature": self.feature_names[idx],
                    "shap_value": round(float(sv[idx]), 4),
                    "feature_value": round(float(feature_vector[0, idx]), 4),
                    "direction": "increases_risk"
                })
        return reasons
