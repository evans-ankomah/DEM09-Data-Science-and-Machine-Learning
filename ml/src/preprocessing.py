"""
Preprocessing Module
====================
Feature/target separation, encoding, scaling, and train-test split.
"""

import logging
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

# Target column
TARGET = 'total_fare_bdt'

# Categorical features for one-hot encoding
CATEGORICAL_FEATURES = [
    'airline', 'source', 'destination', 'class', 'stopovers',
    'booking_source', 'seasonality', 'duration_bucket',
    'booking_lead_bucket', 'aircraft_type'
]

# Numerical features for scaling
NUMERICAL_FEATURES = [
    'duration_hrs', 'days_before_departure',
    'departure_month', 'departure_weekday', 'departure_hour'
]

# Columns to EXCLUDE from features (leakage prevention)
LEAKAGE_COLUMNS = ['base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt']


def prepare_features(df, fit_scaler=True, scaler=None):
    """
    Prepare features from the ML-ready dataframe.

    1. Separates target (total_fare_bdt) from features
    2. Applies one-hot encoding to categorical columns
    3. Scales numerical features with StandardScaler

    Args:
        df: DataFrame with ML features
        fit_scaler: If True, fit a new scaler; if False, use provided scaler
        scaler: Pre-fitted StandardScaler (for prediction)

    Returns:
        X: Feature DataFrame (encoded + scaled)
        y: Target Series
        scaler: Fitted StandardScaler
        feature_names: List of feature column names after encoding
    """
    logger.info(f"Preparing features from {len(df)} rows")

    # Separate target
    if TARGET not in df.columns:
        raise ValueError(f"Target column '{TARGET}' not found in DataFrame")
    y = df[TARGET].copy()

    # Drop target and any leakage columns
    drop_cols = [c for c in LEAKAGE_COLUMNS if c in df.columns]
    X = df.drop(columns=drop_cols, errors='ignore').copy()

    # Also drop non-feature columns that may have leaked through
    non_feature_cols = [
        'id', 'booking_hash', 'departure_datetime', 'arrival_datetime',
        'created_at', 'updated_at', 'processed_at', 'source_name',
        'destination_name'
    ]
    X = X.drop(columns=[c for c in non_feature_cols if c in X.columns],
               errors='ignore')

    logger.info(f"Features before encoding: {list(X.columns)}")

    # One-hot encode categorical features
    cat_cols = [c for c in CATEGORICAL_FEATURES if c in X.columns]
    X = pd.get_dummies(X, columns=cat_cols, drop_first=False, dtype=int)

    # Scale numerical features
    num_cols = [c for c in NUMERICAL_FEATURES if c in X.columns]
    if num_cols:
        if fit_scaler:
            scaler = StandardScaler()
            X[num_cols] = scaler.fit_transform(X[num_cols])
        elif scaler is not None:
            X[num_cols] = scaler.transform(X[num_cols])

    # Fill any remaining NaN with 0
    X = X.fillna(0)

    feature_names = list(X.columns)
    logger.info(f"Features after encoding: {len(feature_names)} columns")
    logger.info(f"Target: '{TARGET}' with {len(y)} values")
    logger.info(f"Target stats: mean={y.mean():.2f}, min={y.min():.2f}, max={y.max():.2f}")

    return X, y, scaler, feature_names


def split_data(X, y, test_size=0.2, random_state=42):
    """
    Split data into train and test sets.

    Args:
        X: Feature DataFrame
        y: Target Series
        test_size: Fraction for test set (default 0.2)
        random_state: Random seed for reproducibility

    Returns:
        X_train, X_test, y_train, y_test
    """
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    logger.info(f"Train: {len(X_train)} rows, Test: {len(X_test)} rows")
    return X_train, X_test, y_train, y_test
