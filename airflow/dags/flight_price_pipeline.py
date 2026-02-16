"""
Flight Price Analysis Pipeline - Airflow DAG (v5)
==================================================

End-to-end ELT + ML pipeline for Bangladesh flight price analysis.
Medallion Architecture (Bronze -> Silver -> Gold) with Postgres + dbt.

Pipeline Flow:
  start -> check_csv -> [BRANCH]
                          |-> (CSV found)    -> load_csv -> validate_bronze
                          |                       -> dbt -> validate_silver -> validate_gold
                          |                       -> train_ml -> end
                          |-> (CSV missing)  -> alert_no_csv -> end

Data Quality Gates:
  - CSV existence: BranchPythonOperator (branch + alert if missing)
  - Bronze/Silver/Gold: Retries + on_failure_callback (Slack + email)

Features:
  - BranchPythonOperator for CSV check with immediate alerting
  - Retries with exponential backoff at every processing stage
  - DAG-level and task-level on_failure_callback (Slack + email)
  - Structured logging throughout
"""

from datetime import datetime, timedelta
import os
import glob
import hashlib
import logging
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ============================================
# Configuration
# ============================================

POSTGRES_CONFIG = {
    'host': os.environ.get('POSTGRES_HOST', 'postgres'),
    'port': int(os.environ.get('POSTGRES_PORT', 5432)),
    'user': os.environ.get('POSTGRES_USER', 'postgres'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'postgres'),
    'dbname': os.environ.get('POSTGRES_DATABASE', 'analytics'),
}

CSV_FOLDER = '/opt/airflow/data/'
CSV_PATTERN = 'Flight_Price_*.csv'
CHUNK_SIZE = 10000

logger = logging.getLogger(__name__)


# ============================================
# Alert Callbacks (Slack + Email)
# ============================================

def _build_alert_message(context, alert_type='failure'):
    """Build a formatted alert message from Airflow context."""
    ti = context.get('task_instance')
    dag_id = ti.dag_id if ti else 'unknown'
    task_id = ti.task_id if ti else 'unknown'
    run_id = context.get('run_id', 'unknown')
    execution_date = context.get('execution_date', 'unknown')
    log_url = ti.log_url if ti else 'N/A'
    exception = context.get('exception', 'No exception info')

    emoji = ':x:' if alert_type == 'failure' else ':warning:'
    title = 'Task Failed' if alert_type == 'failure' else 'Data Quality Alert'

    return (
        f"{emoji} *Airflow {title}*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Run ID:* `{run_id}`\n"
        f"*Execution Date:* `{execution_date}`\n"
        f"*Exception:* `{exception}`\n"
        f"*Log URL:* {log_url}"
    )


def _send_slack_alert(message):
    """Send a Slack alert via webhook (non-blocking)."""
    try:
        from airflow.providers.http.hooks.http import HttpHook
        hook = HttpHook(http_conn_id='slack_webhook', method='POST')
        hook.run(
            endpoint='',
            data=json.dumps({"text": message}),
            headers={'Content-Type': 'application/json'}
        )
        logger.info("Slack alert sent")
    except Exception as e:
        logger.warning(f"Slack alert failed (non-blocking): {e}")


def _send_email_alert(subject, message):
    """Send an email alert (non-blocking)."""
    try:
        from airflow.utils.email import send_email
        send_email(
            to='admin@example.com',
            subject=subject,
            html_content=message.replace('\n', '<br>')
        )
        logger.info("Email alert sent")
    except Exception as e:
        logger.warning(f"Email alert failed (non-blocking): {e}")


def slack_failure_callback(context):
    """
    on_failure_callback: Send Slack + email alert on any task failure.
    Attached to every task via default_args.
    """
    message = _build_alert_message(context, 'failure')
    logger.error(f"ALERT: {message}")

    _send_slack_alert(message)

    ti = context.get('task_instance')
    subject = f"[AIRFLOW ALERT] {ti.dag_id}.{ti.task_id} FAILED"
    _send_email_alert(subject, message)


# ============================================
# Default DAG Arguments
# ============================================

default_args = {
    'owner': 'EvansAnkomah',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),
    'on_failure_callback': slack_failure_callback,
}


# ============================================
# Helper Functions
# ============================================

def get_postgres_connection():
    """Create and return a PostgreSQL database connection."""
    return psycopg2.connect(**POSTGRES_CONFIG)


def generate_booking_hash(row):
    """Generate a unique hash for a booking based on key columns."""
    key_string = '|'.join([
        str(row.get('Airline', '')),
        str(row.get('Source', '')),
        str(row.get('Destination', '')),
        str(row.get('Departure Date & Time', '')),
        str(row.get('Class', '')),
        str(row.get('Booking Source', ''))
    ])
    return hashlib.sha256(key_string.encode()).hexdigest()[:32]


def _check_table_rows(schema, table):
    """Check if a table exists and return its row count. Returns -1 if table missing."""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        if cursor.fetchone()[0] == 0:
            return -1
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        return cursor.fetchone()[0]
    finally:
        cursor.close()
        conn.close()


# ============================================
# BRANCH: Check CSV Exists (BranchPythonOperator)
# ============================================

def check_csv_exists(**context):
    """
    BranchPythonOperator callable:
      - Returns 'load_csv_to_postgres' if CSV files are found
      - Returns 'alert_no_csv' if no CSV files exist (triggers alert branch)
    """
    logger.info(f"BRANCH: Checking for CSV files in {CSV_FOLDER}")

    csv_files = glob.glob(os.path.join(CSV_FOLDER, CSV_PATTERN))
    if not csv_files:
        csv_files = glob.glob(os.path.join(CSV_FOLDER, '*.csv'))

    if not csv_files:
        logger.error(
            f"NO CSV FILES FOUND in {CSV_FOLDER}. "
            f"Branching to alert_no_csv task."
        )
        return 'alert_no_csv'

    # Validate files are not empty
    valid_files = []
    for f in csv_files:
        size = os.path.getsize(f)
        if size == 0:
            logger.warning(f"  Skipping empty file: {f}")
            continue
        valid_files.append(f)
        logger.info(f"  Found: {f} ({size / 1024 / 1024:.1f} MB)")

    if not valid_files:
        logger.error("All CSV files are empty. Branching to alert_no_csv.")
        return 'alert_no_csv'

    logger.info(f"CSV CHECK PASSED: {len(valid_files)} valid file(s)")
    context['ti'].xcom_push(key='csv_files', value=valid_files)
    return 'load_csv_to_postgres'


# ============================================
# Alert Task: No CSV Found (Slack + Email)
# ============================================

def alert_no_csv(**context):
    """
    Alert task triggered when BranchPythonOperator detects no CSV files.
    Sends immediate Slack + email notification and logs the issue.
    """
    message = (
        ":file_folder: *DATA SOURCE MISSING*\n"
        f"*DAG:* `flight_price_pipeline`\n"
        f"*Issue:* No CSV files found in `{CSV_FOLDER}`\n"
        f"*Expected Pattern:* `{CSV_PATTERN}`\n"
        f"*Action Required:* Upload CSV data to the data/ directory "
        f"and re-trigger the DAG.\n"
        f"*Run ID:* `{context.get('run_id', 'unknown')}`\n"
        f"*Timestamp:* `{datetime.now().isoformat()}`"
    )

    logger.error(f"ALERT - NO CSV: {message}")
    _send_slack_alert(message)
    _send_email_alert(
        "[AIRFLOW ALERT] flight_price_pipeline - NO CSV DATA",
        message
    )
    logger.info("Alert sent. Pipeline will skip to end.")


# ============================================
# Task: Load CSV to PostgreSQL Bronze
# ============================================

def load_csv_to_postgres(**context):
    """Load flight price CSV directly into PostgreSQL Bronze layer."""
    logger.info(f"Loading CSV data to Postgres Bronze...")
    start_time = datetime.now()

    csv_files = context['ti'].xcom_pull(key='csv_files', task_ids='check_csv_exists')
    if not csv_files:
        csv_files = glob.glob(os.path.join(CSV_FOLDER, CSV_PATTERN))
        if not csv_files:
            csv_files = glob.glob(os.path.join(CSV_FOLDER, '*.csv'))

    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        # Create schemas
        for schema in ['bronze', 'silver', 'gold']:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.commit()

        # Create Bronze table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_flight_prices (
            id SERIAL PRIMARY KEY,
            booking_hash VARCHAR(32) NOT NULL UNIQUE,
            airline VARCHAR(255),
            source VARCHAR(255),
            source_name VARCHAR(255),
            destination VARCHAR(255),
            destination_name VARCHAR(255),
            departure_datetime VARCHAR(255),
            arrival_datetime VARCHAR(255),
            duration_hrs DECIMAL(10, 4),
            stopovers VARCHAR(50),
            aircraft_type VARCHAR(255),
            class VARCHAR(50),
            booking_source VARCHAR(255),
            base_fare_bdt DECIMAL(15, 6),
            tax_surcharge_bdt DECIMAL(15, 6),
            total_fare_bdt DECIMAL(15, 6),
            seasonality VARCHAR(50),
            days_before_departure INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_booking_hash
            ON bronze.raw_flight_prices (booking_hash)
        """)
        conn.commit()

        total_processed = 0
        for csv_file in csv_files:
            logger.info(f"Processing: {csv_file}")
            df = pd.read_csv(csv_file)
            logger.info(f"  Rows in file: {len(df)}")

            for i in range(0, len(df), CHUNK_SIZE):
                chunk = df.iloc[i:i + CHUNK_SIZE]
                values = []
                for _, row in chunk.iterrows():
                    booking_hash = generate_booking_hash(row)
                    values.append((
                        booking_hash,
                        str(row.get('Airline', '')).strip(),
                        str(row.get('Source', '')).strip(),
                        str(row.get('Source Name', '')).strip(),
                        str(row.get('Destination', '')).strip(),
                        str(row.get('Destination Name', '')).strip(),
                        str(row.get('Departure Date & Time', '')).strip(),
                        str(row.get('Arrival Date & Time', '')).strip(),
                        float(row.get('Duration (hrs)', 0)) if pd.notna(row.get('Duration (hrs)')) else None,
                        str(row.get('Stopovers', '')).strip(),
                        str(row.get('Aircraft Type', '')).strip(),
                        str(row.get('Class', '')).strip(),
                        str(row.get('Booking Source', '')).strip(),
                        float(row.get('Base Fare (BDT)', 0)) if pd.notna(row.get('Base Fare (BDT)')) else None,
                        float(row.get('Tax & Surcharge (BDT)', 0)) if pd.notna(row.get('Tax & Surcharge (BDT)')) else None,
                        float(row.get('Total Fare (BDT)', 0)) if pd.notna(row.get('Total Fare (BDT)')) else None,
                        str(row.get('Seasonality', '')).strip(),
                        int(row.get('Days Before Departure', 0)) if pd.notna(row.get('Days Before Departure')) else None,
                    ))

                upsert_sql = """
                    INSERT INTO bronze.raw_flight_prices (
                        booking_hash, airline, source, source_name,
                        destination, destination_name,
                        departure_datetime, arrival_datetime,
                        duration_hrs, stopovers, aircraft_type,
                        class, booking_source, base_fare_bdt,
                        tax_surcharge_bdt, total_fare_bdt,
                        seasonality, days_before_departure
                    ) VALUES %s
                    ON CONFLICT (booking_hash) DO UPDATE SET
                        base_fare_bdt = EXCLUDED.base_fare_bdt,
                        tax_surcharge_bdt = EXCLUDED.tax_surcharge_bdt,
                        total_fare_bdt = EXCLUDED.total_fare_bdt,
                        updated_at = CURRENT_TIMESTAMP
                """
                execute_values(cursor, upsert_sql, values)
                conn.commit()
                total_processed += len(chunk)
                logger.info(f"  Processed {total_processed} rows...")

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Bronze load complete: {total_processed} rows in {duration:.1f}s")
        context['ti'].xcom_push(key='bronze_rows_loaded', value=total_processed)
        return total_processed

    finally:
        cursor.close()
        conn.close()


# ============================================
# GATE 2: Validate Bronze Layer
# ============================================

def validate_bronze(**context):
    """
    DATA GATE: Validate Bronze layer has data and passes quality checks.
    Raises ValueError on failure (triggers retry + alert).
    """
    logger.info("GATE 2: Validating Bronze layer...")
    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        # Check row count
        row_count = _check_table_rows('bronze', 'raw_flight_prices')
        if row_count <= 0:
            raise ValueError(
                f"BRONZE GATE FAILED: bronze.raw_flight_prices has {row_count} rows. "
                f"Data loading may have failed."
            )
        logger.info(f"  Row count: {row_count}")

        # Check for nulls in critical columns
        for col in ['airline', 'source', 'destination', 'booking_hash']:
            cursor.execute(
                f"SELECT COUNT(*) FROM bronze.raw_flight_prices "
                f"WHERE {col} IS NULL OR {col} = ''"
            )
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                logger.warning(f"  {null_count} null/empty values in '{col}'")

        # Check for negative fares
        cursor.execute("""
            SELECT COUNT(*) FROM bronze.raw_flight_prices
            WHERE base_fare_bdt < 0 OR tax_surcharge_bdt < 0 OR total_fare_bdt < 0
        """)
        neg_count = cursor.fetchone()[0]
        if neg_count > 0:
            logger.warning(f"  {neg_count} rows with negative fares (will be cleaned in Silver)")

        # Check duplicates
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT booking_hash FROM bronze.raw_flight_prices
                GROUP BY booking_hash HAVING COUNT(*) > 1
            ) dups
        """)
        dup_count = cursor.fetchone()[0]
        if dup_count > 0:
            logger.warning(f"  {dup_count} duplicate booking hashes found")

        logger.info(f"GATE 2 PASSED: Bronze layer validated ({row_count} rows)")
        context['ti'].xcom_push(key='bronze_row_count', value=row_count)
        return row_count

    finally:
        cursor.close()
        conn.close()


# ============================================
# GATE 3: Validate Silver Layer
# ============================================

def validate_silver(**context):
    """
    DATA GATE: Validate Silver layer after dbt transformations.
    Raises ValueError if dbt produced 0 rows (triggers retry + alert).
    """
    logger.info("GATE 3: Validating Silver layer...")

    row_count = _check_table_rows('silver', 'stg_flight_prices')

    if row_count == -1:
        raise ValueError(
            "SILVER GATE FAILED: silver.stg_flight_prices table does not exist. "
            "dbt Silver model may have failed to materialize."
        )

    if row_count == 0:
        raise ValueError(
            "SILVER GATE FAILED: silver.stg_flight_prices has 0 rows. "
            "dbt Silver model ran but produced no output - check data quality filters."
        )

    bronze_count = context['ti'].xcom_pull(key='bronze_row_count', task_ids='validate_bronze') or 0
    if bronze_count > 0:
        drop_pct = round((1 - row_count / bronze_count) * 100, 1)
        logger.info(f"  Bronze→Silver: {bronze_count} → {row_count} rows ({drop_pct}% filtered)")
        if drop_pct > 50:
            logger.warning(f"  HIGH DATA LOSS: {drop_pct}% of rows dropped in Silver!")

    logger.info(f"GATE 3 PASSED: Silver layer validated ({row_count} rows)")
    context['ti'].xcom_push(key='silver_row_count', value=row_count)
    return row_count


# ============================================
# GATE 4: Validate Gold Layer (ML Features)
# ============================================

def validate_gold(**context):
    """
    DATA GATE: Validate Gold ML features table exists and has data.
    Raises ValueError if ml_features has 0 rows (triggers retry + alert).
    """
    logger.info("GATE 4: Validating Gold layer (ml_features)...")

    row_count = _check_table_rows('gold', 'ml_features')

    if row_count == -1:
        raise ValueError(
            "GOLD GATE FAILED: gold.ml_features table does not exist. "
            "dbt Gold model may have failed to materialize."
        )

    if row_count == 0:
        raise ValueError(
            "GOLD GATE FAILED: gold.ml_features has 0 rows. "
            "No feature-ready data available for ML training."
        )

    # Also check other gold KPI tables
    gold_tables = [
        'avg_fare_by_airline', 'avg_fare_by_class', 'avg_fare_by_route',
        'booking_count_by_airline', 'seasonal_fare_variation', 'top_routes'
    ]
    for table in gold_tables:
        count = _check_table_rows('gold', table)
        status = f"{count} rows" if count >= 0 else "MISSING"
        logger.info(f"  gold.{table}: {status}")

    logger.info(f"GATE 4 PASSED: Gold ML features ready ({row_count} rows)")
    context['ti'].xcom_push(key='gold_row_count', value=row_count)
    return row_count


# ============================================
# Task: Train ML Model
# ============================================

def train_ml_model(**context):
    """
    Run the ML training pipeline.
    Reads from Postgres gold.ml_features, trains models, saves artifacts.
    """
    import sys
    sys.path.insert(0, '/opt/airflow')
    from ml.src.train import run_training_pipeline

    logger.info("Starting ML training pipeline...")
    metrics_df, _ = run_training_pipeline(data_source='postgres')
    logger.info("ML training complete!")

    best = metrics_df.iloc[0]
    context['ti'].xcom_push(key='best_model', value={
        'name': best['model'],
        'r2': float(best['r2']),
        'mae': float(best['mae']),
        'rmse': float(best['rmse'])
    })


# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id='flight_price_pipeline',
    default_args=default_args,
    description=(
        'End-to-end ELT + ML pipeline with BranchPythonOperator for CSV check '
        'and data quality gates at every layer (CSV → Bronze → Silver → Gold → ML)'
    ),
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flight', 'elt', 'ml', 'bangladesh', 'medallion', 'data-quality'],
    on_failure_callback=slack_failure_callback,
) as dag:

    # ---- Start ----
    task_start = EmptyOperator(task_id='start')

    # ---- BRANCH: CSV Check ----
    task_check_csv = BranchPythonOperator(
        task_id='check_csv_exists',
        python_callable=check_csv_exists,
        doc_md="""
        ## Branch: CSV Data Check
        Uses BranchPythonOperator to check for CSV files.
        - **CSV found** → continues to `load_csv_to_postgres`
        - **CSV missing** → branches to `alert_no_csv` → `end`
        """,
    )

    # ---- Alert: No CSV (branch target) ----
    task_alert_no_csv = PythonOperator(
        task_id='alert_no_csv',
        python_callable=alert_no_csv,
        doc_md="Sends Slack + email alert when no CSV files are found.",
    )

    # ---- Load to Bronze ----
    task_load = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres,
        doc_md="Load CSV data into PostgreSQL Bronze layer with upsert.",
    )

    # ---- GATE 2: Bronze Validation ----
    task_validate_bronze = PythonOperator(
        task_id='validate_bronze',
        python_callable=validate_bronze,
        doc_md="""
        ## Gate 2: Bronze Layer Validation
        Checks row count, nulls, negative fares, and duplicates.
        **Failure**: Retries 2x, then sends Slack + email alert.
        """,
    )

    # ---- dbt Transformations (Bronze view + Silver + Gold) ----
    task_dbt = BashOperator(
        task_id='run_dbt_transformations',
        bash_command='''
            cd /opt/airflow/dbt && \
            echo "Installing dbt dependencies..." && \
            dbt deps --profiles-dir . && \
            echo "Running dbt models (Bronze + Silver + Gold)..." && \
            dbt run --profiles-dir . && \
            echo "Running dbt tests..." && \
            dbt test --profiles-dir . && \
            echo "dbt transformations complete!"
        ''',
        doc_md="Run dbt Bronze (view), Silver (cleaning), and Gold (KPIs + ML features) transformations.",
    )

    # ---- GATE 3: Silver Validation ----
    task_validate_silver = PythonOperator(
        task_id='validate_silver',
        python_callable=validate_silver,
        doc_md="""
        ## Gate 3: Silver Layer Validation
        Checks dbt Silver output has rows. Reports Bronze→Silver data loss.
        **Failure**: Retries 2x, then sends Slack + email alert.
        """,
    )

    # ---- GATE 4: Gold Validation ----
    task_validate_gold = PythonOperator(
        task_id='validate_gold',
        python_callable=validate_gold,
        doc_md="""
        ## Gate 4: Gold Layer Validation
        Checks ml_features and all KPI tables have data.
        **Failure**: Retries 2x, then sends Slack + email alert.
        """,
    )

    # ---- ML Training ----
    task_ml = PythonOperator(
        task_id='train_ml_model',
        python_callable=train_ml_model,
        execution_timeout=timedelta(hours=2),
        doc_md="Train 6 ML models, tune hyperparameters, save best model + metrics.",
    )

    # ---- End (with trigger_rule for branch convergence) ----
    task_end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success',
    )

    # ---- Pipeline Flow ----
    # Branch pattern:
    #   start → check_csv → [BRANCH]
    #                          ├→ load_csv → bronze_gate → dbt → silver_gate → gold_gate → ml → end
    #                          └→ alert_no_csv → end
    task_start >> task_check_csv

    # Branch 1: CSV found → full pipeline
    task_check_csv >> task_load >> task_validate_bronze >> task_dbt
    task_dbt >> task_validate_silver >> task_validate_gold >> task_ml >> task_end

    # Branch 2: CSV missing → alert then end
    task_check_csv >> task_alert_no_csv >> task_end
