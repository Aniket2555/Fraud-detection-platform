"""
Synthetic Fraud Generator: Produces realistic minority-class synthetic fraud samples.
"""

import pandas as pd
import numpy as np
from imblearn.over_sampling import SMOTE


def augment_training_data_smote(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    target_ratio: float = 0.2,
    max_synthetic_real_ratio: float = 3.0
) -> tuple:
    """
    Augments minority fraud class up to target_ratio (e.g. 20% positive samples).
    Enforces synthetic:real fraud cap.
    """
    real_fraud_count = int(y_train.sum())
    majority_count = len(y_train) - real_fraud_count

    if real_fraud_count == 0:
        print("No real fraud samples present — SMOTE has nothing to interpolate from, skipping augmentation.")
        return X_train, y_train

    current_ratio = real_fraud_count / majority_count
    max_synthetic = int(real_fraud_count * max_synthetic_real_ratio)
    desired_total_fraud = int(target_ratio * len(y_train) / (1.0 - target_ratio))
    actual_target = min(desired_total_fraud, real_fraud_count + max_synthetic)

    actual_ratio = actual_target / majority_count
    if actual_ratio <= current_ratio:
        # SMOTE can only oversample the minority class; a target_ratio at or
        # below the dataset's existing fraud prevalence would otherwise raise
        # ValueError inside fit_resample.
        print(f"Requested target_ratio implies a fraud ratio ({actual_ratio:.4f}) at or below "
              f"the current ratio ({current_ratio:.4f}) — SMOTE cannot shrink the minority class, "
              "skipping augmentation.")
        return X_train, y_train

    smote = SMOTE(sampling_strategy=actual_ratio, random_state=42)
    X_resampled, y_resampled = smote.fit_resample(X_train, y_train)

    synthetic_added = len(y_resampled) - len(y_train)
    print(f"Original: {X_train.shape[0]} rows ({real_fraud_count} fraud)")
    print(f"After SMOTE: {X_resampled.shape[0]} rows (+{synthetic_added} synthetic)")

    return X_resampled, y_resampled
