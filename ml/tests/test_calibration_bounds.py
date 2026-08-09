"""
Unit tests asserting all calibrated scores lie strictly within [0.0, 1.0].
"""

import pytest
import numpy as np

from fraud_detection.ml.training.calibrate_model import ScoreCalibrator


def test_isotonic_calibration_bounds():
    raw_scores = np.array([-5.0, -1.0, 0.0, 2.5, 10.0, 100.0])
    y_val = np.array([0, 0, 0, 1, 1, 1])

    calibrator = ScoreCalibrator("isotonic").fit(raw_scores, y_val)
    calibrated = calibrator.transform(raw_scores)

    assert np.all(calibrated >= 0.0)
    assert np.all(calibrated <= 1.0)
