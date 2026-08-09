"""
Scikit-Learn Isolation Forest for Outlier Detection.
Trained on legitimate transaction features.
"""

from sklearn.ensemble import IsolationForest
import numpy as np
import math


def train_isolation_forest(
    X_legit_train: np.ndarray,
    n_estimators: int = 300,
    contamination: float = 0.001
) -> IsolationForest:
    """Trains Isolation Forest on legitimate transactions."""
    n_features = X_legit_train.shape[1]
    iso_forest = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        max_samples="auto",
        max_features=max(1, int(math.sqrt(n_features))),
        random_state=42,
        n_jobs=-1
    )
    iso_forest.fit(X_legit_train)
    return iso_forest


def get_isolation_scores(model: IsolationForest, X: np.ndarray) -> np.ndarray:
    """Inverts score so higher value indicates higher anomaly/fraud risk."""
    raw_scores = model.decision_function(X)
    return -raw_scores
