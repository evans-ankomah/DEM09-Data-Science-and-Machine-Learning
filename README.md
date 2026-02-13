# Flight Fare Prediction - End-to-End ELT + ML Pipeline

An industry-ready pipeline for predicting Bangladesh domestic flight fares using **Airflow + PostgreSQL + dbt + scikit-learn**, fully **Dockerized** with a **Streamlit** deployment.

## Architecture

```
CSV Data --> Airflow DAG --> PostgreSQL Bronze --> dbt Silver --> dbt Gold
                                                                    |
                                                            gold.ml_features
                                                                    |
                                                        ML Training (6 models)
                                                                    |
                                                        best_model.pkl --> Streamlit App
```

**Medallion Architecture:**
- **Bronze**: Raw CSV data loaded into `bronze.raw_flight_prices` with deduplication via booking hash
- **Silver**: Cleaned & standardized in `silver.stg_flight_prices` (incremental)
- **Gold**: KPI tables + `gold.ml_features` (ML feature-ready, leakage-free)

## Project Structure

```
flight-price-airflow-dbt/
├── airflow/dags/                    # Airflow DAG (pipeline orchestration)
│   └── flight_price_pipeline.py
├── dbt/                             # dbt project (Silver + Gold layers)
│   ├── models/silver/               # Cleaned data
│   ├── models/gold/                 # KPIs + ML features
│   ├── dbt_project.yml
│   └── profiles.yml
├── ml/                              # Machine Learning
│   ├── src/                         # Reusable Python modules
│   │   ├── data_loader.py           # Load from Postgres or CSV
│   │   ├── preprocessing.py         # Feature engineering + encoding
│   │   ├── train.py                 # Train 6 models with tuning
│   │   ├── evaluate.py              # R2, MAE, RMSE metrics
│   │   └── visualize.py             # 10+ reusable plot functions
│   └── notebooks/
│       └── eda.ipynb                # Exploratory Data Analysis
├── streamlit/
│   └── app.py                       # Live prediction web app
├── docker/
│   ├── Dockerfile                   # Custom Airflow image + ML deps
│   └── docker-compose.yml           # Full stack orchestration
├── data/                            # Raw CSV dataset
├── output/                          # Auto-generated outputs
│   ├── figures/                     # EDA + model plots
│   └── metrics/                     # Model comparison CSV
├── models/                          # Saved ML model artifacts
├── requirements.txt
├── README.md
└── USER_GUIDE.md
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow 2.7 |
| Database | PostgreSQL 13 |
| Transformations | dbt-postgres 1.7 |
| ML Framework | scikit-learn 1.3 |
| Visualization | matplotlib, seaborn |
| Deployment | Streamlit |
| Containerization | Docker Compose |

## Quick Start

### Prerequisites
- Docker & Docker Compose installed
- ~4GB free disk space

### Run

```bash
# Clone and navigate to project
cd flight-price-airflow-dbt

# Start all services
docker compose -f docker/docker-compose.yml up --build

# Wait for initialization (~2 min), then access:
# Airflow UI: http://localhost:8080 (user: airflow, pass: airflow)
# Streamlit:  http://localhost:8501
```

### Trigger the Pipeline

1. Open Airflow at `http://localhost:8080`
2. Find DAG `flight_price_pipeline`
3. Click "Trigger DAG" (play button)
4. Monitor task progress: `start → load → validate → dbt → data_gate → ml_training → end`

### View Results

- **Metrics**: `output/metrics/metrics.csv`
- **Plots**: `output/figures/`
- **Model**: `models/best_model.pkl`
- **Live predictions**: `http://localhost:8501`

## ML Models

| Model | Description |
|-------|-------------|
| Linear Regression | Baseline model |
| Ridge | L2 regularization |
| Lasso | L1 regularization |
| Decision Tree | Non-linear tree-based |
| Random Forest | Ensemble of trees |
| Gradient Boosting | Sequential boosting |

All models are tuned with `RandomizedSearchCV` and compared via cross-validation.

**Features used**: airline, source, destination, class, stopovers, booking source, seasonality, duration, days before departure, departure month/weekday/hour, duration bucket, booking lead bucket.

**Excluded** (leakage prevention): `base_fare_bdt`, `tax_surcharge_bdt` (since total = base + tax).

## Airflow DAG Features

- **Retries**: 3 retries with exponential backoff (2min → 30min max)
- **Data gate**: Checks `gold.ml_features` for rows before ML training; skips gracefully if empty
- **Slack alerts**: DAG-level `on_failure_callback` sends failure details (DAG, task, run_id, log URL)
- **Execution timeout**: 2 hours per task

## Dataset

**Flight Price Dataset of Bangladesh** (Kaggle)
- ~100K+ flight records
- Features: airline, source, destination, class, fare, duration, season, etc.
