# User Guide - Flight Fare Prediction Pipeline

## Table of Contents
1. [System Requirements](#system-requirements)
2. [Installation & Setup](#installation--setup)
3. [Running the Pipeline](#running-the-pipeline)
4. [Using the Streamlit App](#using-the-streamlit-app)
5. [Running the EDA Notebook](#running-the-eda-notebook)
6. [Running ML Training Locally](#running-ml-training-locally)
7. [Understanding the Pipeline](#understanding-the-pipeline)
8. [Troubleshooting](#troubleshooting)
9. [Configuration Reference](#configuration-reference)

---

## System Requirements

- **Docker Desktop** (v20.10+) with Docker Compose
- **RAM**: Minimum 4GB allocated to Docker
- **Disk**: ~4GB free space
- **OS**: Windows 10/11, macOS, or Linux

---

## Installation & Setup

### 1. Ensure Docker is Running

```bash
docker --version
docker compose version
```

### 2. Navigate to Project Directory

```bash
cd flight-price-airflow-dbt
```

### 3. Verify Data File

Ensure `data/Flight_Price_Dataset_of_Bangladesh.csv` exists. If not, download from [Kaggle](https://www.kaggle.com/datasets) and place it in the `data/` folder.

---

## Running the Pipeline

### Start All Services

```bash
docker compose -f docker/docker-compose.yml up --build
```

This starts:
- **PostgreSQL** (analytics database) on port `5433`
- **PostgreSQL** (Airflow metadata) internal
- **Airflow Webserver** on port `8080`
- **Airflow Scheduler**
- **Streamlit App** on port `8501`

### First-Time Initialization

On first run, the `airflow-init` container will:
1. Initialize the Airflow database
2. Create admin user (`airflow`/`airflow`)
3. Install dbt dependencies

Wait ~2 minutes for all services to become healthy.

### Trigger the DAG

1. Open **http://localhost:8080**
2. Login with `airflow` / `airflow`
3. Find `flight_price_pipeline` in the DAGs list
4. Click the **play button** (Trigger DAG)
5. Monitor progress in the **Grid** or **Graph** view

### Pipeline Tasks

| Task | What It Does |
|------|-------------|
| `start` | Entry point |
| `load_csv_to_postgres` | Loads CSV into PostgreSQL Bronze layer with deduplication |
| `validate_data` | Validates row counts, nulls, duplicates, negative fares |
| `run_dbt_transformations` | Runs dbt (Silver cleaning + Gold KPIs + ML features) |
| `check_data_gate` | Checks if `gold.ml_features` has data; branches accordingly |
| `train_ml_model` | Trains 6 ML models, saves best model + metrics + plots |
| `skip_ml` | Executed if data gate fails (no data) |
| `end` | Pipeline completion |

### Stop Services

```bash
docker compose -f docker/docker-compose.yml down
```

To remove volumes (reset database):
```bash
docker compose -f docker/docker-compose.yml down -v
```

---

## Using the Streamlit App

Access at **http://localhost:8501** after the pipeline has run at least once.

### Predict Tab
1. Select flight details (airline, source, destination, class, etc.)
2. Set duration, days before departure, month, and hour
3. Click **Predict Fare**
4. View the predicted fare in BDT

### Model Performance Tab
- View comparison table of all trained models
- See R2, MAE, and RMSE for each model
- Visual comparison charts

### Analysis Tab
- Browse all EDA and model evaluation plots generated during training

---

## Running the EDA Notebook

### Option A: Inside Docker

```bash
docker exec -it airflow-scheduler bash
cd /opt/airflow
pip install jupyter
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root
```

### Option B: Locally

```bash
pip install pandas numpy matplotlib seaborn scikit-learn jupyter
cd flight-price-airflow-dbt
jupyter notebook ml/notebooks/eda.ipynb
```

---

## Running ML Training Locally

If you want to run ML training without Docker (using CSV data):

```bash
# Install dependencies
pip install -r requirements.txt

# Run training (auto-detects CSV fallback)
cd flight-price-airflow-dbt
python -m ml.src.train csv
```

Outputs:
- `output/figures/` - All plots
- `output/metrics/metrics.csv` - Model comparison
- `models/best_model.pkl` - Best model artifact

---

## Understanding the Pipeline

### Data Flow

```
1. CSV File (data/)
   ↓
2. PostgreSQL Bronze (bronze.raw_flight_prices)
   - Raw data with booking_hash for deduplication
   - Upsert support for incremental loads
   ↓
3. dbt Silver (silver.stg_flight_prices)
   - Cleaned, trimmed, validated
   - Duration buckets, booking lead buckets
   - Incremental materialization
   ↓
4. dbt Gold (gold.*)
   - avg_fare_by_airline
   - avg_fare_by_class/route
   - seasonal_fare_variation
   - top_routes, booking_count_by_airline
   - ml_features (leakage-free feature table)
   ↓
5. ML Training
   - 6 models with hyperparameter tuning
   - Saves best_model.pkl + metrics + plots
   ↓
6. Streamlit App
   - Live prediction using best_model.pkl
```

### ML Feature Engineering

**Categorical** (one-hot encoded):
- airline, source, destination, class, stopovers
- booking_source, seasonality, aircraft_type
- duration_bucket, booking_lead_bucket

**Numerical** (standard-scaled):
- duration_hrs, days_before_departure
- departure_month, departure_weekday, departure_hour

**Excluded** (leakage prevention):
- base_fare_bdt, tax_surcharge_bdt

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Containers won't start | Check Docker is running: `docker ps` |
| Airflow UI not loading | Wait 2+ min for init; check: `docker logs airflow-webserver` |
| DAG not visible | Check `airflow/dags/` is mounted; restart scheduler |
| dbt fails | Check Postgres is healthy: `docker logs postgres` |
| ML training slow | Normal for first run (~5-15 min depending on data size) |
| Streamlit shows no model | Run the full pipeline at least once first |
| Port conflicts | Change ports in `docker/docker-compose.yml` |

---

## Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `postgres` | Analytics database host |
| `POSTGRES_PORT` | `5432` | Analytics database port |
| `POSTGRES_USER` | `postgres` | Database username |
| `POSTGRES_PASSWORD` | `postgres` | Database password |
| `POSTGRES_DATABASE` | `analytics` | Database name |

### Ports

| Service | Port | URL |
|---------|------|-----|
| Airflow | 8080 | http://localhost:8080 |
| Streamlit | 8501 | http://localhost:8501 |
| PostgreSQL | 5433 | localhost:5433 |

### Airflow Credentials
- Username: `airflow`
- Password: `airflow`
