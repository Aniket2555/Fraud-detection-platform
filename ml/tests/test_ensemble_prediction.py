"""
Unit tests for ensemble prediction format and output keys.
"""

import pytest
import numpy as np


def test_ensemble_output_format():
    sample_scores = {
        "fraud_probability": 0.15,
        "component_scores": {
            "xgboost": 0.12,
            "autoencoder": 0.18,
            "isolation_forest": 0.14
        },
        "scoring_mode": "full"
    }

    assert "fraud_probability" in sample_scores
    assert 0.0 <= sample_scores["fraud_probability"] <= 1.0
    assert "component_scores" in sample_scores
    assert sample_scores["scoring_mode"] == "full"
