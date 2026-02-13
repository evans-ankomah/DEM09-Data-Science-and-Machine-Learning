"""
Training Module
===============
Trains multiple regression models, performs hyperparameter tuning,
and saves the best model.

Usage:
    python -m ml.src.train          (from project root)
    python ml/src/train.py          (direct execution)
"""

import os
import sys
import logging
import warnings
import joblib
import numpy as np
import pandas as pd

from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import RandomizedSearchCV, cross_val_score

# Add project root to path for imports
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from ml.src.data_loader import load_data
from ml.src.preprocessing import prepare_features, split_data
from ml.src.evaluate import evaluate_all_models, save_metrics
from ml.src.visualize import (
    plot_fare_distribution,
    plot_fare_by_airline,
    plot_avg_fare_by_season,
    plot_correlation_heatmap,
    plot_predicted_vs_actual,
    plot_feature_importance,
    plot_model_comparison,
    plot_residuals,
)

# Suppress convergence warnings during training
warnings.filterwarnings('ignore', category=UserWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Output paths
MODELS_DIR = os.path.join(_PROJECT_ROOT, 'models')
FIGURES_DIR = os.path.join(_PROJECT_ROOT, 'output', 'figures')
METRICS_DIR = os.path.join(_PROJECT_ROOT, 'output', 'metrics')


def get_models():
    """Return dict of model name -> (estimator, param_grid)."""
    return {
        'Linear Regression': (
            LinearRegression(),
            {}  # No hyperparameters to tune
        ),
        'Ridge': (
            Ridge(),
            {'alpha': [0.01, 0.1, 1.0, 10.0, 100.0]}
        ),
        'Lasso': (
            Lasso(max_iter=5000),
            {'alpha': [0.01, 0.1, 1.0, 10.0, 100.0]}
        ),
        'Decision Tree': (
            DecisionTreeRegressor(random_state=42),
            {
                'max_depth': [5, 10, 15, 20, None],
                'min_samples_split': [2, 5, 10],
                'min_samples_leaf': [1, 2, 4],
            }
        ),
        'Random Forest': (
            RandomForestRegressor(random_state=42, n_jobs=-1),
            {
                'n_estimators': [50, 100, 200],
                'max_depth': [10, 15, 20, None],
                'min_samples_split': [2, 5],
                'min_samples_leaf': [1, 2],
            }
        ),
        'Gradient Boosting': (
            GradientBoostingRegressor(random_state=42),
            {
                'n_estimators': [100, 200],
                'max_depth': [3, 5, 7],
                'learning_rate': [0.01, 0.05, 0.1],
                'subsample': [0.8, 1.0],
            }
        ),
    }


def train_all_models(X_train, y_train, cv=5, n_iter=20):
    """
    Train all models with hyperparameter tuning.

    Args:
        X_train, y_train: training data
        cv: cross-validation folds
        n_iter: iterations for RandomizedSearchCV

    Returns:
        dict of {model_name: best_fitted_model}
        dict of {model_name: cv_scores}
    """
    models = get_models()
    trained = {}
    cv_scores = {}

    for name, (estimator, param_grid) in models.items():
        logger.info(f"\n{'='*50}")
        logger.info(f"Training: {name}")
        logger.info(f"{'='*50}")

        if param_grid:
            # Use RandomizedSearchCV for models with hyperparameters
            search = RandomizedSearchCV(
                estimator, param_grid, n_iter=min(n_iter, _param_combinations(param_grid)),
                cv=cv, scoring='r2', n_jobs=-1, random_state=42, verbose=0
            )
            search.fit(X_train, y_train)
            best_model = search.best_estimator_
            logger.info(f"Best params: {search.best_params_}")
            logger.info(f"Best CV R2: {search.best_score_:.4f}")
        else:
            # Simple fit for models without hyperparameters
            best_model = estimator
            best_model.fit(X_train, y_train)

        # Cross-validation scores
        scores = cross_val_score(best_model, X_train, y_train, cv=cv,
                                 scoring='r2', n_jobs=-1)
        cv_scores[name] = {
            'mean_r2': round(scores.mean(), 4),
            'std_r2': round(scores.std(), 4),
            'scores': scores.tolist()
        }
        logger.info(f"CV R2: {scores.mean():.4f} (+/- {scores.std():.4f})")

        trained[name] = best_model

    return trained, cv_scores


def _param_combinations(param_grid):
    """Count total number of parameter combinations."""
    total = 1
    for values in param_grid.values():
        total *= len(values)
    return total


def save_best_model(trained_models, metrics_df, scaler, feature_names):
    """Save the best model based on R2 score."""
    os.makedirs(MODELS_DIR, exist_ok=True)

    best_row = metrics_df.loc[metrics_df['r2'].idxmax()]
    best_name = best_row['model']
    best_model = trained_models[best_name]

    artifact = {
        'model': best_model,
        'model_name': best_name,
        'scaler': scaler,
        'feature_names': feature_names,
        'metrics': best_row.to_dict(),
    }

    filepath = os.path.join(MODELS_DIR, 'best_model.pkl')
    joblib.dump(artifact, filepath)
    logger.info(f"\nBest model saved: {best_name}")
    logger.info(f"  R2={best_row['r2']:.4f}, MAE={best_row['mae']:.2f}, RMSE={best_row['rmse']:.2f}")
    logger.info(f"  Saved to: {filepath}")
    return filepath


def run_training_pipeline(data_source='auto'):
    """
    Execute the full ML training pipeline:
    1. Load data
    2. Preprocess features
    3. Train & tune all models
    4. Evaluate on test set
    5. Generate plots
    6. Save best model + metrics
    """
    logger.info("=" * 60)
    logger.info("FLIGHT FARE PREDICTION - ML TRAINING PIPELINE")
    logger.info("=" * 60)

    # Ensure output directories exist
    os.makedirs(FIGURES_DIR, exist_ok=True)
    os.makedirs(METRICS_DIR, exist_ok=True)
    os.makedirs(MODELS_DIR, exist_ok=True)

    # 1. Load data
    logger.info("\n--- Step 1: Loading Data ---")
    df = load_data(source=data_source)
    logger.info(f"Dataset shape: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")

    # 2. Generate EDA plots before preprocessing
    logger.info("\n--- Step 2: EDA Visualizations ---")
    try:
        plot_fare_distribution(df, output_dir=FIGURES_DIR)
        if 'airline' in df.columns:
            plot_fare_by_airline(df, output_dir=FIGURES_DIR)
        if 'seasonality' in df.columns:
            plot_avg_fare_by_season(df, output_dir=FIGURES_DIR)
        plot_correlation_heatmap(df, output_dir=FIGURES_DIR)
    except Exception as e:
        logger.warning(f"Some EDA plots failed: {e}")

    # 3. Preprocess
    logger.info("\n--- Step 3: Preprocessing ---")
    X, y, scaler, feature_names = prepare_features(df)
    X_train, X_test, y_train, y_test = split_data(X, y)

    # 4. Train all models
    logger.info("\n--- Step 4: Training Models ---")
    trained_models, cv_scores = train_all_models(X_train, y_train)

    # 5. Evaluate on test set
    logger.info("\n--- Step 5: Evaluating Models ---")
    metrics_df, predictions = evaluate_all_models(trained_models, X_test, y_test)

    # 6. Save metrics
    logger.info("\n--- Step 6: Saving Results ---")
    save_metrics(metrics_df, METRICS_DIR)

    # Add CV scores to metrics
    cv_df = pd.DataFrame([
        {'model': name, 'cv_mean_r2': v['mean_r2'], 'cv_std_r2': v['std_r2']}
        for name, v in cv_scores.items()
    ])
    full_metrics = metrics_df.merge(cv_df, on='model', how='left')
    save_metrics(full_metrics, METRICS_DIR)

    # 7. Generate model evaluation plots
    logger.info("\n--- Step 7: Model Evaluation Plots ---")
    try:
        # Model comparison chart
        plot_model_comparison(metrics_df, output_dir=FIGURES_DIR)

        # Best model plots
        best_name = metrics_df.iloc[0]['model']
        best_preds = predictions[best_name]
        best_model = trained_models[best_name]

        plot_predicted_vs_actual(y_test, best_preds, best_name,
                                output_dir=FIGURES_DIR)
        plot_residuals(y_test, best_preds, best_name, output_dir=FIGURES_DIR)

        # Feature importance (for tree-based models)
        if hasattr(best_model, 'feature_importances_'):
            plot_feature_importance(
                best_model.feature_importances_, feature_names,
                best_name, output_dir=FIGURES_DIR
            )
        elif hasattr(best_model, 'coef_'):
            plot_feature_importance(
                np.abs(best_model.coef_), feature_names,
                best_name, output_dir=FIGURES_DIR
            )
    except Exception as e:
        logger.warning(f"Some evaluation plots failed: {e}")

    # 8. Save best model
    save_best_model(trained_models, metrics_df, scaler, feature_names)

    logger.info("\n" + "=" * 60)
    logger.info("TRAINING PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Figures saved to: {FIGURES_DIR}")
    logger.info(f"Metrics saved to: {METRICS_DIR}")
    logger.info(f"Model saved to: {MODELS_DIR}")

    return metrics_df, trained_models


if __name__ == '__main__':
    # Allow command-line override of data source
    source = sys.argv[1] if len(sys.argv) > 1 else 'auto'
    run_training_pipeline(data_source=source)
