"""
Unit tests for ML feature engineering pipeline.
"""

import pytest
import pandas as pd
import numpy as np

from fraud_detection.ml.training.utils.feature_engineering import FraudFeatureEngineer


def test_feature_engineering_transform():
    data = pd.DataFrame({
        "transaction_amt": [100.0, 50.5],
        "log_amount": [4.615, 3.941],
        "amount_cents": [0, 50],
        "is_round_amount": [1, 0],
        "event_hour": [14, 2],
        "event_day_of_week": [1, 6],
        "product_cd": ["W", "C"],
        "card4": ["visa", "mastercard"],
        "card6": ["debit", "credit"],
        "device_type": ["desktop", np.nan]
    })

    fe = FraudFeatureEngineer()
    fe.fit(data)
    transformed = fe.transform(data)

    assert transformed.shape[0] == 2
    assert "is_weekend" in transformed.columns
    assert "is_night" in transformed.columns
    assert "has_identity" in transformed.columns
    assert transformed.loc[0, "is_night"] == 0
    assert transformed.loc[1, "is_night"] == 1
    assert transformed.loc[0, "has_identity"] == 1
    assert transformed.loc[1, "has_identity"] == 0
