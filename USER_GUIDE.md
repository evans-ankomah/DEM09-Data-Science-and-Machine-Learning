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

### Pipeline Tasks

| Task | Description | Duration |
|------|-------------|----------|
| 1. start | Pipeline entry point (DummyOperator) | ~1 sec |
| 2. load_csv_to_mysql | Loads 57,000 rows to MySQL Bronze | ~30 sec |
| 3. validate_mysql_data | Validates columns, nulls, data types | ~5 sec |
| 4. transfer_to_postgres_bronze | Transfers data to PostgreSQL | ~15 sec |
| 5. run_dbt_transformations | Builds Silver + Gold tables | ~30 sec |
| 6. end | Pipeline completion (DummyOperator) | ~1 sec |

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
docker exec postgres psql -U postgres -d analytics -c "SELECT COUNT(*) FROM silver.stg_flight_prices;"
```

**Expected: 57,000 rows**

### Step 5.3: Check New Transformations

**Duration Buckets:**
```powershell
docker exec postgres psql -U postgres -d analytics -c "SELECT DISTINCT duration_bucket FROM silver.stg_flight_prices ORDER BY 1;"
```

**Expected values:**
- Long (6+h)
- Medium (3-6h)
- Short (0-3h)

**Booking Lead Buckets:**
```powershell
docker exec postgres psql -U postgres -d analytics -c "SELECT DISTINCT booking_lead_bucket FROM silver.stg_flight_prices ORDER BY 1;"
```

**Expected values:**
- Early Bird (30+ days)
- Last Minute (0-3 days)
- Short Notice (4-14 days)
- Standard (15-30 days)

### Step 5.4: Check Gold Layer Tables

```powershell
docker exec postgres psql -U postgres -d analytics -c "
SELECT 'avg_fare_by_airline' as table_name, COUNT(*) as rows FROM gold.avg_fare_by_airline
UNION ALL
SELECT 'avg_fare_by_class', COUNT(*) FROM gold.avg_fare_by_class
UNION ALL
SELECT 'avg_fare_by_route', COUNT(*) FROM gold.avg_fare_by_route
UNION ALL
SELECT 'booking_count_by_airline', COUNT(*) FROM gold.booking_count_by_airline
UNION ALL
SELECT 'top_routes', COUNT(*) FROM gold.top_routes
UNION ALL
SELECT 'seasonal_fare_variation', COUNT(*) FROM gold.seasonal_fare_variation;
"
```

### Step 5.5: View Sample Results

**Average Fare by Class:**
```powershell
docker exec postgres psql -U postgres -d analytics -c "SELECT * FROM gold.avg_fare_by_class;"
```

**Top Routes:**
```powershell
docker exec postgres psql -U postgres -d analytics -c "SELECT route, booking_count, avg_fare_bdt FROM gold.avg_fare_by_route LIMIT 10;"
```

### Step 5.6: Run dbt Tests (Optional)

```powershell
docker exec -u airflow airflow-scheduler bash -c "cd /opt/airflow/dbt_project && /home/airflow/.local/bin/dbt test --profiles-dir ."
```

**Expected: PASS=XX WARN=0 ERROR=0**

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
PostgreSQL Silver (silver.stg_flight_prices)
    - Cleaned data
    - duration_bucket (Short/Medium/Long)
    - booking_lead_bucket (Last Minute/Standard/Early Bird)
    ↓
PostgreSQL Gold (KPI tables)
    - avg_fare_by_airline
    - avg_fare_by_class
    - avg_fare_by_route
    - booking_count_by_airline
    - top_routes
    - seasonal_fare_variation
```

### Gold Layer Tables

| Table | Description |
|-------|-------------|
| avg_fare_by_airline | Average fare per airline |
| avg_fare_by_class | Average fare by travel class (Economy/Business/First) |
| avg_fare_by_route | Average fare per route with price-per-hour metric |
| booking_count_by_airline | Total bookings and market share by airline |
| top_routes | Most popular routes by booking count |
| seasonal_fare_variation | Fare patterns by seasonality |

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
docker exec postgres psql -U postgres -d analytics -c "SELECT DISTINCT seasonality FROM silver.stg_flight_prices;"
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
