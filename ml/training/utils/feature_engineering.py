"""
Batch feature engineering for Phase 1 baseline model.

Production decisions:
- Feature engineering pipeline is a scikit-learn Pipeline (serializable, reproducible)
- Encoding maps are fitted on train split ONLY, then applied to val/test
- V-column selection uses variance threshold, not manual selection
- Missing value indicators are explicit features
"""

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import OrdinalEncoder
from sklearn.feature_selection import VarianceThreshold


class VColumnSelector(BaseEstimator, TransformerMixin):
    """Select V-columns with variance above threshold."""
    def __init__(self, variance_threshold=0.01, null_threshold=0.95):
        self.variance_threshold = variance_threshold
        self.null_threshold = null_threshold
        self.selected_columns = None

    def fit(self, X, y=None):
        null_rates = X.isnull().mean()
        low_null_cols = null_rates[null_rates < self.null_threshold].index.tolist()

        X_filtered = X[low_null_cols].fillna(0)
        if X_filtered.shape[1] > 0:
            vt = VarianceThreshold(threshold=self.variance_threshold)
            vt.fit(X_filtered)
            self.selected_columns = [
                low_null_cols[i] for i, keep in enumerate(vt.get_support()) if keep
            ]
        else:
            self.selected_columns = []
        return self

    def transform(self, X):
        return X[self.selected_columns]


class FraudFeatureEngineer(BaseEstimator, TransformerMixin):
    """Complete feature engineering pipeline for the baseline model."""
    def __init__(self):
        self.low_card_encoder = None
        self.v_selector = None
        self.feature_names = None

        self.numeric_cols = [
            'transaction_amt', 'log_amount', 'amount_cents', 'is_round_amount',
            'event_hour', 'event_day_of_week',
            'addr1', 'addr2', 'dist1', 'dist2',
        ]
        self.count_cols = [f'c{i}' for i in range(1, 15)]
        self.delta_cols = [f'd{i}' for i in range(1, 16)]
        self.match_cols = [f'm{i}' for i in range(1, 10)]
        self.null_flag_cols = [
            'addr1_is_null', 'addr2_is_null', 'dist1_is_null', 'dist2_is_null'
        ] + [f'c{i}_is_null' for i in range(1, 15)]
        self.low_card_cols = ['product_cd', 'card4', 'card6']
        self.v_cols = [f'v{i}' for i in range(1, 340)]

    def fit(self, X, y=None):
        existing_low_card = [c for c in self.low_card_cols if c in X.columns]
        if existing_low_card:
            self.low_card_encoder = OrdinalEncoder(
                handle_unknown='use_encoded_value', unknown_value=-1
            )
            self.low_card_encoder.fit(X[existing_low_card].astype(str).fillna('unknown'))

        existing_v_cols = [c for c in self.v_cols if c in X.columns]
        if existing_v_cols:
            self.v_selector = VColumnSelector()
            self.v_selector.fit(X[existing_v_cols])

        return self

    def transform(self, X):
        parts = []

        numeric_existing = [c for c in self.numeric_cols if c in X.columns]
        if numeric_existing:
            parts.append(X[numeric_existing].fillna(-999))

        count_existing = [c for c in self.count_cols if c in X.columns]
        if count_existing:
            parts.append(X[count_existing].fillna(-999))

        delta_existing = [c for c in self.delta_cols if c in X.columns]
        if delta_existing:
            parts.append(X[delta_existing].fillna(-999))

        match_existing = [c for c in self.match_cols if c in X.columns]
        if match_existing:
            parts.append(X[match_existing].fillna(-1))

        flag_existing = [c for c in self.null_flag_cols if c in X.columns]
        if flag_existing:
            parts.append(X[flag_existing].fillna(0))

        existing_low_card = [c for c in self.low_card_cols if c in X.columns]
        if self.low_card_encoder is not None and existing_low_card:
            low_card_encoded = pd.DataFrame(
                self.low_card_encoder.transform(X[existing_low_card].astype(str).fillna('unknown')),
                columns=[f"{c}_encoded" for c in existing_low_card],
                index=X.index
            )
            parts.append(low_card_encoded)

        if self.v_selector is not None:
            existing_v = [c for c in self.v_cols if c in X.columns]
            if existing_v:
                v_selected = self.v_selector.transform(X[existing_v]).fillna(-999)
                parts.append(v_selected)

        derived = pd.DataFrame(index=X.index)
        if 'event_day_of_week' in X.columns:
            derived['is_weekend'] = X['event_day_of_week'].isin([5, 6]).astype(int)
        if 'event_hour' in X.columns:
            derived['is_night'] = X['event_hour'].isin(range(0, 6)).astype(int)
        if 'device_type' in X.columns:
            derived['has_identity'] = X['device_type'].notna().astype(int)
        parts.append(derived)

        result = pd.concat(parts, axis=1)
        self.feature_names = result.columns.tolist()
        return result
