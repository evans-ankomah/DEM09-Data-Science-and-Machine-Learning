"""
Evaluate Module
===============
Compute and save model evaluation metrics.
"""

import os
import logging
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

logger = logging.getLogger(__name__)

METRICS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    'output', 'metrics'
)


def evaluate_model(model, X_test, y_test, model_name='Model'):
    """
    Evaluate a trained model on test data.

    Returns:
        dict with model name, R2, MAE, RMSE
    """
    y_pred = model.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    metrics = {
        'model': model_name,
        'r2': round(r2, 6),
        'mae': round(mae, 2),
        'rmse': round(rmse, 2),
    }
    logger.info(f"{model_name}: R2={r2:.4f}, MAE={mae:.2f}, RMSE={rmse:.2f}")
    return metrics, y_pred


def evaluate_all_models(models_dict, X_test, y_test):
    """
    Evaluate all models and return a comparison DataFrame.

    Args:
        models_dict: {name: fitted_model}
        X_test, y_test: test data

    Returns:
        DataFrame with columns: model, r2, mae, rmse
        dict of {model_name: y_pred}
    """
    results = []
    predictions = {}
    for name, model in models_dict.items():
        metrics, y_pred = evaluate_model(model, X_test, y_test, name)
        results.append(metrics)
        predictions[name] = y_pred

    metrics_df = pd.DataFrame(results).sort_values('r2', ascending=False)
    logger.info(f"\nModel Comparison:\n{metrics_df.to_string(index=False)}")
    return metrics_df, predictions


def save_metrics(metrics_df, output_dir=None):
    """Save metrics DataFrame to CSV."""
    out_dir = output_dir or METRICS_DIR
    os.makedirs(out_dir, exist_ok=True)
    filepath = os.path.join(out_dir, 'metrics.csv')
    metrics_df.to_csv(filepath, index=False)
    logger.info(f"Saved metrics to: {filepath}")
    return filepath
