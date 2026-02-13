"""
Visualization Module
====================
Reusable plot functions for EDA and model evaluation.
All functions save figures to output/figures/ and return fig objects.
"""

import os
import logging
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for headless environments
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Default output directory
FIGURES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    'output', 'figures'
)

# Style configuration
sns.set_theme(style='whitegrid', font_scale=1.1)
plt.rcParams.update({
    'figure.figsize': (12, 7),
    'figure.dpi': 150,
    'savefig.bbox': 'tight',
    'axes.titlesize': 14,
    'axes.labelsize': 12,
})


def _save_fig(fig, filename, output_dir=None):
    """Save figure to the output directory."""
    out_dir = output_dir or FIGURES_DIR
    os.makedirs(out_dir, exist_ok=True)
    filepath = os.path.join(out_dir, filename)
    fig.savefig(filepath, bbox_inches='tight', dpi=150)
    logger.info(f"Saved figure: {filepath}")
    return filepath


def plot_fare_distribution(df, column='total_fare_bdt', output_dir=None):
    """Plot histogram of fare distribution."""
    fig, ax = plt.subplots(figsize=(12, 7))
    ax.hist(df[column].dropna(), bins=50, color='#2196F3', edgecolor='white',
            alpha=0.85)
    ax.set_title('Total Fare Distribution (BDT)', fontweight='bold')
    ax.set_xlabel('Total Fare (BDT)')
    ax.set_ylabel('Frequency')
    ax.axvline(df[column].mean(), color='red', linestyle='--',
               label=f'Mean: {df[column].mean():,.0f} BDT')
    ax.axvline(df[column].median(), color='orange', linestyle='--',
               label=f'Median: {df[column].median():,.0f} BDT')
    ax.legend()
    plt.tight_layout()
    _save_fig(fig, 'fare_distribution.png', output_dir)
    return fig


def plot_fare_by_airline(df, output_dir=None):
    """Plot boxplot of fare by airline."""
    fig, ax = plt.subplots(figsize=(14, 8))
    airlines = df.groupby('airline')['total_fare_bdt'].median().sort_values(
        ascending=False
    ).index
    sns.boxplot(
        data=df, x='airline', y='total_fare_bdt', order=airlines,
        palette='Set2', ax=ax, showfliers=False
    )
    ax.set_title('Fare Distribution by Airline', fontweight='bold')
    ax.set_xlabel('Airline')
    ax.set_ylabel('Total Fare (BDT)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    _save_fig(fig, 'fare_by_airline_boxplot.png', output_dir)
    return fig


def plot_avg_fare_by_season(df, output_dir=None):
    """Plot average fare by season (bar chart)."""
    season_avg = df.groupby('seasonality')['total_fare_bdt'].mean().sort_values(
        ascending=False
    )
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(season_avg.index, season_avg.values, color='#FF9800',
                  edgecolor='white')
    ax.set_title('Average Fare by Season', fontweight='bold')
    ax.set_xlabel('Season')
    ax.set_ylabel('Average Total Fare (BDT)')
    for bar, val in zip(bars, season_avg.values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 50,
                f'{val:,.0f}', ha='center', va='bottom', fontsize=10)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    _save_fig(fig, 'avg_fare_by_season.png', output_dir)
    return fig


def plot_avg_fare_by_month(df, output_dir=None):
    """Plot average fare by month."""
    if 'departure_month' not in df.columns:
        logger.warning("departure_month column not found, skipping plot")
        return None
    monthly = df.groupby('departure_month')['total_fare_bdt'].mean()
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(monthly.index, monthly.values, marker='o', color='#4CAF50',
            linewidth=2, markersize=8)
    ax.fill_between(monthly.index, monthly.values, alpha=0.2, color='#4CAF50')
    ax.set_title('Average Fare by Month', fontweight='bold')
    ax.set_xlabel('Month')
    ax.set_ylabel('Average Total Fare (BDT)')
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels([
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
    ])
    plt.tight_layout()
    _save_fig(fig, 'avg_fare_by_month.png', output_dir)
    return fig


def plot_correlation_heatmap(df, output_dir=None):
    """Plot correlation heatmap of numerical features."""
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if len(numeric_cols) < 2:
        logger.warning("Not enough numerical columns for heatmap")
        return None
    corr = df[numeric_cols].corr()
    fig, ax = plt.subplots(figsize=(12, 10))
    mask = np.triu(np.ones_like(corr, dtype=bool))
    sns.heatmap(
        corr, mask=mask, annot=True, fmt='.2f', cmap='RdBu_r',
        center=0, square=True, linewidths=0.5, ax=ax,
        cbar_kws={'shrink': 0.8}
    )
    ax.set_title('Feature Correlation Heatmap', fontweight='bold')
    plt.tight_layout()
    _save_fig(fig, 'correlation_heatmap.png', output_dir)
    return fig


def plot_predicted_vs_actual(y_true, y_pred, model_name='Model', output_dir=None):
    """Plot predicted vs actual values scatter plot."""
    fig, ax = plt.subplots(figsize=(10, 10))
    ax.scatter(y_true, y_pred, alpha=0.3, s=10, color='#2196F3')
    # Perfect prediction line
    lims = [
        min(min(y_true), min(y_pred)),
        max(max(y_true), max(y_pred))
    ]
    ax.plot(lims, lims, 'r--', linewidth=2, label='Perfect Prediction')
    ax.set_title(f'Predicted vs Actual - {model_name}', fontweight='bold')
    ax.set_xlabel('Actual Total Fare (BDT)')
    ax.set_ylabel('Predicted Total Fare (BDT)')
    ax.legend()
    ax.set_aspect('equal')
    plt.tight_layout()
    safe_name = model_name.lower().replace(' ', '_')
    _save_fig(fig, f'predicted_vs_actual_{safe_name}.png', output_dir)
    return fig


def plot_feature_importance(importances, feature_names, model_name='Model',
                            top_n=20, output_dir=None):
    """Plot horizontal bar chart of feature importances."""
    # Sort by importance
    indices = np.argsort(importances)[-top_n:]
    fig, ax = plt.subplots(figsize=(12, max(6, top_n * 0.35)))
    ax.barh(
        range(len(indices)),
        importances[indices],
        color='#4CAF50', edgecolor='white'
    )
    ax.set_yticks(range(len(indices)))
    ax.set_yticklabels([feature_names[i] for i in indices])
    ax.set_title(f'Top {top_n} Feature Importances - {model_name}',
                 fontweight='bold')
    ax.set_xlabel('Importance')
    plt.tight_layout()
    safe_name = model_name.lower().replace(' ', '_')
    _save_fig(fig, f'feature_importance_{safe_name}.png', output_dir)
    return fig


def plot_model_comparison(metrics_df, output_dir=None):
    """Plot model comparison bar chart from metrics DataFrame."""
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    metrics = ['r2', 'mae', 'rmse']
    colors = ['#4CAF50', '#FF9800', '#F44336']
    titles = ['R-squared (higher is better)',
              'MAE (lower is better)',
              'RMSE (lower is better)']

    for ax, metric, color, title in zip(axes, metrics, colors, titles):
        if metric in metrics_df.columns:
            data = metrics_df.sort_values(metric,
                                          ascending=(metric != 'r2'))
            ax.barh(data['model'], data[metric], color=color, edgecolor='white')
            ax.set_title(title, fontweight='bold')
            ax.set_xlabel(metric.upper())
            for i, v in enumerate(data[metric]):
                ax.text(v, i, f' {v:.4f}', va='center', fontsize=9)

    plt.suptitle('Model Performance Comparison', fontweight='bold', fontsize=14)
    plt.tight_layout()
    _save_fig(fig, 'model_comparison.png', output_dir)
    return fig


def plot_residuals(y_true, y_pred, model_name='Model', output_dir=None):
    """Plot residuals distribution."""
    residuals = np.array(y_true) - np.array(y_pred)
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Residual scatter
    axes[0].scatter(y_pred, residuals, alpha=0.3, s=10, color='#9C27B0')
    axes[0].axhline(y=0, color='red', linestyle='--', linewidth=2)
    axes[0].set_title(f'Residuals vs Predicted - {model_name}', fontweight='bold')
    axes[0].set_xlabel('Predicted Fare (BDT)')
    axes[0].set_ylabel('Residual (BDT)')

    # Residual histogram
    axes[1].hist(residuals, bins=50, color='#9C27B0', edgecolor='white', alpha=0.85)
    axes[1].set_title(f'Residual Distribution - {model_name}', fontweight='bold')
    axes[1].set_xlabel('Residual (BDT)')
    axes[1].set_ylabel('Frequency')
    axes[1].axvline(x=0, color='red', linestyle='--', linewidth=2)

    plt.tight_layout()
    safe_name = model_name.lower().replace(' ', '_')
    _save_fig(fig, f'residuals_{safe_name}.png', output_dir)
    return fig
