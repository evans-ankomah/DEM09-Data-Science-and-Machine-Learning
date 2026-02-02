# 📘 Flight Price Analysis Pipeline - User Guide

## Complete Step-by-Step Instructions

This guide walks you through running the entire Flight Price Analysis pipeline from start to finish.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Project Setup](#2-project-setup)
3. [Starting the Pipeline](#3-starting-the-pipeline)
4. [Running the DAG](#4-running-the-dag)
5. [Verifying Results](#5-verifying-results)
6. [Understanding the Output](#6-understanding-the-output)
7. [Stopping the Pipeline](#7-stopping-the-pipeline)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Prerequisites

Before starting, ensure you have:

| Requirement | Version | Check Command |
|-------------|---------|---------------|
| Docker | 20.10+ | `docker --version` |
| Docker Compose | 2.0+ | `docker-compose --version` |
| RAM | 4GB+ available | - |
| Disk Space | 5GB+ free | - |

---

## 2. Project Setup

### Step 2.1: Navigate to Project Folder

```powershell
cd "c:\Users\EvansAnkomah\Downloads\py\Airflow 2\flight-price-airflow-dbt"
```

### Step 2.2: Verify CSV File is in Place

The CSV file should be in `include/data/`:

```powershell
dir include\data\
```

**Expected output:**
```
Flight_Price_Dataset_of_Bangladesh.csv    (about 14 MB)
```

If missing, copy your CSV file:
```powershell
copy "path\to\your\Flight_Price_Dataset_of_Bangladesh.csv" "include\data\"
```

### Step 2.3: Verify Project Structure

```powershell
dir
```

You should see:
```
dags/
dbt_project/
include/
logs/
docker-compose.yml
README.md
requirements.txt
```

---

## 3. Starting the Pipeline

### Step 3.1: Start Docker Containers

```powershell
docker-compose up -d
```

**Expected output:**
```
[+] Running 6/6
 ✔ Container mysql             Started
 ✔ Container postgres          Started
 ✔ Container postgres-airflow  Started
 ✔ Container airflow-init      Started
 ✔ Container airflow-scheduler Started
 ✔ Container airflow-webserver Started
```

### Step 3.2: Wait for Initialization

Wait 2-3 minutes for all services to fully initialize.

### Step 3.3: Verify All Containers Are Running

```powershell
docker-compose ps
```

**Expected output:**
```
NAME                STATUS
airflow-scheduler   Up (healthy or unhealthy - both OK)
airflow-webserver   Up (healthy)
mysql               Up (healthy)
postgres            Up (healthy)
postgres-airflow    Up (healthy)
```

### Step 3.4: Install dbt in Airflow Container (First Time Only)

```powershell
docker exec -u airflow airflow-scheduler python -m pip install --user dbt-postgres==1.7.0
```

Wait ~2 minutes for installation to complete.

### Step 3.5: Install dbt Dependencies

```powershell
docker exec -u airflow airflow-scheduler bash -c "cd /opt/airflow/dbt_project && /home/airflow/.local/bin/dbt deps --profiles-dir ."
```

---

## 4. Running the DAG

### Option A: Using Airflow Web UI (Recommended)

1. **Open browser**: http://localhost:8080

2. **Login**:
   - Username: `airflow`
   - Password: `airflow`

3. **Find the DAG**: Look for `flight_price_pipeline`

4. **Enable the DAG**: Click the toggle switch to "On" (if grey/paused)

5. **Trigger the DAG**: Click the ▶️ Play button

6. **Monitor**: Watch the Graph view for task progress

### Option B: Using Command Line

```powershell
# Trigger the DAG
docker exec airflow-scheduler airflow dags trigger flight_price_pipeline
```

### What Happens During Execution

| Task | Description | Duration |
|------|-------------|----------|
| 1. load_csv_to_mysql | Loads 57,000 rows to MySQL Bronze | ~30 sec |
| 2. validate_mysql_data | Validates columns, nulls, data types | ~5 sec |
| 3. transfer_to_postgres_bronze | Transfers data to PostgreSQL | ~15 sec |
| 4. run_dbt_transformations | Builds Silver + Gold tables | ~30 sec |

**Total expected runtime: 1-2 minutes**

---

## 5. Verifying Results

### Step 5.1: Check Bronze Layer (MySQL)

```powershell
docker exec mysql mysql -u root -proot flight_bronze -e "SELECT COUNT(*) as rows FROM raw_flight_prices;"
```

**Expected: 57,000 rows**

### Step 5.2: Check Silver Layer (PostgreSQL)

```powershell
docker exec postgres psql -U postgres -d analytics -c "SELECT COUNT(*) FROM public_silver.stg_flight_prices;"
```

**Expected: 57,000 rows**

### Step 5.3: Check Gold Layer Tables

```powershell
docker exec postgres psql -U postgres -d analytics -c "
SELECT 'avg_fare_by_airline' as table_name, COUNT(*) as rows FROM public_gold.avg_fare_by_airline
UNION ALL
SELECT 'booking_count_by_airline', COUNT(*) FROM public_gold.booking_count_by_airline
UNION ALL
SELECT 'top_routes', COUNT(*) FROM public_gold.top_routes
UNION ALL
SELECT 'seasonal_fare_variation', COUNT(*) FROM public_gold.seasonal_fare_variation
UNION ALL
SELECT 'seasonal_fare_variation_gh', COUNT(*) FROM public_gold.seasonal_fare_variation_gh;
"
```

### Step 5.4: View Ghana-like Seasonality Results

```powershell
docker exec postgres psql -U postgres -d analytics -c "
SELECT seasonality_gh, booking_count, avg_total_fare_bdt, season_type 
FROM public_gold.seasonal_fare_variation_gh 
ORDER BY avg_total_fare_bdt DESC;
"
```

### Step 5.5: Run dbt Tests (Optional)

```powershell
docker exec -u airflow airflow-scheduler bash -c "cd /opt/airflow/dbt_project && /home/airflow/.local/bin/dbt test --profiles-dir ."
```

**Expected: PASS=23 WARN=0 ERROR=0**

---

## 6. Understanding the Output

### Data Flow

```
CSV File (57,000 rows)
    ↓
MySQL Bronze (raw_flight_prices)
    ↓
PostgreSQL Bronze (bronze.raw_flight_prices)
    ↓
PostgreSQL Silver (public_silver.stg_flight_prices)
    - Cleaned data
    - Ghana-like seasonality added
    ↓
PostgreSQL Gold (KPI tables)
    - avg_fare_by_airline
    - booking_count_by_airline
    - top_routes
    - seasonal_fare_variation
    - seasonal_fare_variation_gh
```

### Ghana-like Seasonality Mapping

| Season | Description | Fare Impact |
|--------|-------------|-------------|
| Hajj_like | Pilgrimage season | Highest fares |
| Eid_like | Festival season | Very high fares |
| Christmas_NewYear | Holiday period | High fares |
| Easter | Spring holiday | Moderate-high |
| Regular | Normal periods | Baseline fares |

---

## 7. Stopping the Pipeline

### Stop All Containers (Keep Data)

```powershell
docker-compose down
```

### Stop and Remove All Data

```powershell
docker-compose down -v
```

### Restart After Stopping

```powershell
docker-compose up -d
```

---

## 8. Troubleshooting

### Issue: Container Won't Start

```powershell
# Check logs
docker-compose logs airflow-webserver
docker-compose logs mysql
docker-compose logs postgres

# Restart containers
docker-compose restart
```

### Issue: Port 8080 Already in Use

```powershell
# Find what's using port 8080
netstat -ano | findstr :8080

# Stop the process or change port in docker-compose.yml
```

### Issue: dbt Command Not Found

```powershell
# Reinstall dbt
docker exec -u airflow airflow-scheduler python -m pip install --user dbt-postgres==1.7.0
```

### Issue: Database Connection Refused

Wait 30-60 seconds after `docker-compose up` for databases to fully initialize, then retry.

### Issue: DAG Not Showing in Airflow UI

```powershell
# Check for DAG parsing errors
docker exec airflow-scheduler airflow dags list

# Check scheduler logs
docker-compose logs airflow-scheduler | tail -50
```

### Issue: dbt Tests Failing

Check if actual data values match schema.yml accepted_values:

```powershell
docker exec postgres psql -U postgres -d analytics -c "SELECT DISTINCT seasonality FROM public_silver.stg_flight_prices;"
```

---

## Quick Reference Commands

| Action | Command |
|--------|---------|
| Start all | `docker-compose up -d` |
| Stop all | `docker-compose down` |
| View logs | `docker-compose logs -f` |
| Trigger DAG | `docker exec airflow-scheduler airflow dags trigger flight_price_pipeline` |
| Check DAG status | `docker exec airflow-scheduler airflow dags list-runs -d flight_price_pipeline` |
| Run dbt | `docker exec -u airflow airflow-scheduler bash -c "cd /opt/airflow/dbt_project && /home/airflow/.local/bin/dbt run --profiles-dir ."` |
| Run dbt tests | `docker exec -u airflow airflow-scheduler bash -c "cd /opt/airflow/dbt_project && /home/airflow/.local/bin/dbt test --profiles-dir ."` |

---

## Summary Checklist

- [ ] Docker and Docker Compose installed
- [ ] CSV file in `include/data/`
- [ ] Run `docker-compose up -d`
- [ ] Wait 2-3 minutes
- [ ] Install dbt (first time only)
- [ ] Open http://localhost:8080 and login
- [ ] Trigger `flight_price_pipeline` DAG
- [ ] Verify results in database
- [ ] Stop with `docker-compose down`
