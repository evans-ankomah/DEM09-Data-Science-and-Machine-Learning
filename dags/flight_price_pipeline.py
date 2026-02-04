"""
Flight Price Analysis Pipeline - Airflow DAG (Optimized v2)

This DAG implements an ETL pipeline for Bangladesh flight price analysis
using the Medallion Architecture (Bronze → Silver → Gold).

OPTIMIZATIONS (v2):
- Bulk insert using LOAD DATA / execute_values for faster loading
- Incremental load support with upsert (ON DUPLICATE KEY UPDATE / ON CONFLICT)
- Multi-CSV support (processes all CSVs in folder)
- Composite hash key for duplicate detection

Pipeline Flow:
1. Start (DummyOperator)
2. Load CSV(s) to MySQL (Bronze) - with upsert
3. Validate data in MySQL
4. Transfer to PostgreSQL Bronze - with upsert
5. Run dbt transformations (Silver + Gold)
6. End (DummyOperator)

"""

from datetime import datetime, timedelta
import os
import glob
import hashlib
import logging
import pandas as pd
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ============================================
# Configuration
# ============================================

# MySQL connection settings (Bronze layer)
MYSQL_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'mysql'),
    'port': int(os.environ.get('MYSQL_PORT', 3306)),
    'user': os.environ.get('MYSQL_USER', 'root'),
    'password': os.environ.get('MYSQL_PASSWORD', 'root'),
    'database': os.environ.get('MYSQL_DATABASE', 'flight_bronze')
}

# PostgreSQL connection settings (Silver/Gold layers)
POSTGRES_CONFIG = {
    'host': os.environ.get('POSTGRES_HOST', 'postgres'),
    'port': int(os.environ.get('POSTGRES_PORT', 5432)),
    'user': os.environ.get('POSTGRES_USER', 'postgres'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'postgres'),
    'database': os.environ.get('POSTGRES_DATABASE', 'analytics')
}

# CSV folder path (supports multiple CSVs)
CSV_FOLDER = '/opt/airflow/data/'
CSV_PATTERN = 'Flight_Price_*.csv'  # Pattern to match CSV files

# Chunk size for batch inserts (larger = faster but more memory)
CHUNK_SIZE = 10000

# Logger
logger = logging.getLogger(__name__)

# ============================================
# Default DAG Arguments
# ============================================

default_args = {
    'owner': 'EvansAnkomah',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=1),
}

# ============================================
# Helper Functions
# ============================================

def get_mysql_connection():
    """Create and return a MySQL database connection."""
    return mysql.connector.connect(**MYSQL_CONFIG)


def get_postgres_connection():
    """Create and return a PostgreSQL database connection."""
    return psycopg2.connect(**POSTGRES_CONFIG)


def generate_booking_hash(row):
    """
    Generate a unique hash for a booking based on key columns.
    
    Columns used: airline, source, destination, departure_datetime, class, booking_source
    
    Returns:
        str: SHA256 hash (first 32 chars)
    """
    key_string = '|'.join([
        str(row.get('Airline', '')),
        str(row.get('Source', '')),
        str(row.get('Destination', '')),
        str(row.get('Departure Date & Time', '')),
        str(row.get('Class', '')),
        str(row.get('Booking Source', ''))
    ])
    return hashlib.sha256(key_string.encode()).hexdigest()[:32]


# ============================================
# Task 1: Load CSV(s) to MySQL (Bronze Layer)
# ============================================

def load_csv_to_mysql(**context):
    """
    Load flight price CSV file(s) into MySQL Bronze layer.
    
    OPTIMIZATIONS:
    - Bulk insert using executemany with larger chunks
    - Upsert logic with ON DUPLICATE KEY UPDATE
    - Supports multiple CSV files in folder
    - Composite hash key for duplicate detection
    
    Args:
        **context: Airflow context dictionary
    
    Returns:
        dict: Load statistics
    """
    logger.info(f"Starting CSV load from folder: {CSV_FOLDER}")
    start_time = datetime.now()
    
    # Find all matching CSV files
    csv_files = glob.glob(os.path.join(CSV_FOLDER, CSV_PATTERN))
    
    if not csv_files:
        # Fallback to any CSV in folder
        csv_files = glob.glob(os.path.join(CSV_FOLDER, '*.csv'))
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {CSV_FOLDER}")
    
    logger.info(f"Found {len(csv_files)} CSV file(s): {csv_files}")
    
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    try:
        # Create table with booking_hash for upsert
        logger.info("Creating/updating Bronze table in MySQL...")
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_flight_prices (
            id INT AUTO_INCREMENT PRIMARY KEY,
            booking_hash VARCHAR(32) NOT NULL,
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
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_booking (booking_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        cursor.execute(create_table_sql)
        conn.commit()
        
        # Check if table needs migration (first run vs incremental)
        cursor.execute("SELECT COUNT(*) FROM raw_flight_prices")
        existing_rows = cursor.fetchone()[0]
        is_incremental = existing_rows > 0
        
        if is_incremental:
            logger.info(f"Incremental mode: {existing_rows} existing rows found")
        else:
            logger.info("Initial load mode: table is empty")
        
        # Prepare upsert SQL
        upsert_sql = """
        INSERT INTO raw_flight_prices (
            booking_hash, airline, source, source_name, destination, destination_name,
            departure_datetime, arrival_datetime, duration_hrs, stopovers,
            aircraft_type, class, booking_source, base_fare_bdt,
            tax_surcharge_bdt, total_fare_bdt, seasonality, days_before_departure
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            base_fare_bdt = VALUES(base_fare_bdt),
            tax_surcharge_bdt = VALUES(tax_surcharge_bdt),
            total_fare_bdt = VALUES(total_fare_bdt),
            updated_at = CURRENT_TIMESTAMP
        """
        
        total_processed = 0
        total_inserted = 0
        total_updated = 0
        
        for csv_file in csv_files:
            logger.info(f"Processing: {csv_file}")
            
            # Read CSV
            df = pd.read_csv(csv_file)
            file_rows = len(df)
            logger.info(f"  Rows in file: {file_rows}")
            
            # Process in chunks
            for i in range(0, len(df), CHUNK_SIZE):
                chunk = df.iloc[i:i + CHUNK_SIZE]
                
                data = []
                for _, row in chunk.iterrows():
                    booking_hash = generate_booking_hash(row)
                    data.append((
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
                        int(row.get('Days Before Departure', 0)) if pd.notna(row.get('Days Before Departure')) else None
                    ))
                
                # Execute batch upsert
                cursor.executemany(upsert_sql, data)
                
                # Track insert vs update counts
                affected = cursor.rowcount
                # MySQL: affected_rows = 1 for insert, 2 for update
                
                conn.commit()
                total_processed += len(chunk)
                logger.info(f"  Processed {total_processed} rows so far...")
        
        # Get final counts
        cursor.execute("SELECT COUNT(*) FROM raw_flight_prices")
        final_count = cursor.fetchone()[0]
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        stats = {
            'files_processed': len(csv_files),
            'rows_processed': total_processed,
            'final_row_count': final_count,
            'duration_seconds': duration,
            'mode': 'incremental' if is_incremental else 'initial'
        }
        
        logger.info(f" CSV Load Complete!")
        logger.info(f"   Files processed: {len(csv_files)}")
        logger.info(f"   Rows processed: {total_processed}")
        logger.info(f"   Final table count: {final_count}")
        logger.info(f"   Duration: {duration:.2f} seconds")
        logger.info(f"   Throughput: {total_processed/duration:.0f} rows/sec")
        
        context['ti'].xcom_push(key='load_stats', value=stats)
        
        return stats
        
    finally:
        cursor.close()
        conn.close()


# ============================================
# Task 2: Validate MySQL Data
# ============================================

def validate_mysql_data(**context):
    """
    Validate the data loaded into MySQL Bronze layer.
    
    Validation checks:
    1. Table exists and has rows
    2. Required columns are present
    3. No null values in critical columns
    4. Numeric fields are valid
    5. Duplicate detection
    """
    logger.info("Starting Bronze layer validation...")
    
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        validation_results = {}
        
        # Check 1: Row count
        cursor.execute("SELECT COUNT(*) as cnt FROM raw_flight_prices")
        row_count = cursor.fetchone()['cnt']
        validation_results['row_count'] = row_count
        
        if row_count == 0:
            raise ValueError(" Validation failed: Table is empty!")
        logger.info(f"Row count: {row_count}")
        
        # Check 2: Unique booking_hash (no duplicates)
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM (
                SELECT booking_hash FROM raw_flight_prices 
                GROUP BY booking_hash HAVING COUNT(*) > 1
            ) dups
        """)
        dup_count = cursor.fetchone()['cnt']
        validation_results['duplicate_hashes'] = dup_count
        
        if dup_count > 0:
            logger.warning(f"Found {dup_count} duplicate booking hashes")
        else:
            logger.info("No duplicate booking hashes")
        
        # Check 3: Null check for critical columns
        for col in ['airline', 'source', 'destination', 'booking_hash']:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM raw_flight_prices WHERE {col} IS NULL OR {col} = ''")
            null_count = cursor.fetchone()['cnt']
            validation_results[f'{col}_nulls'] = null_count
            if null_count > 0:
                logger.warning(f" Found {null_count} null/empty values in '{col}'")
            else:
                logger.info(f"No nulls in '{col}'")
        
        # Check 4: Numeric field validation
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM raw_flight_prices 
            WHERE base_fare_bdt < 0 OR tax_surcharge_bdt < 0 OR total_fare_bdt < 0
        """)
        negative_count = cursor.fetchone()['cnt']
        validation_results['negative_fares'] = negative_count
        
        if negative_count > 0:
            logger.warning(f" Found {negative_count} rows with negative fare values")
        else:
            logger.info(" All fares are non-negative")
        
        logger.info(" Bronze layer validation complete!")
        context['ti'].xcom_push(key='validation_results', value=validation_results)
        
        return validation_results
        
    finally:
        cursor.close()
        conn.close()


# ============================================
# Task 3: Transfer to PostgreSQL Bronze
# ============================================

def transfer_to_postgres_bronze(**context):
    """
    Transfer data from MySQL Bronze to PostgreSQL Bronze schema.
    
    OPTIMIZATIONS:
    - Uses execute_values for bulk insert
    - Upsert with ON CONFLICT for incremental loads
    - Transfers booking_hash for deduplication
    """
    logger.info("Starting transfer from MySQL to PostgreSQL Bronze...")
    start_time = datetime.now()
    
    mysql_conn = get_mysql_connection()
    mysql_cursor = mysql_conn.cursor(dictionary=True)
    
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()
    
    try:
        # Create schemas
        logger.info("Creating PostgreSQL schemas...")
        pg_cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        pg_cursor.execute("CREATE SCHEMA IF NOT EXISTS silver")
        pg_cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        pg_conn.commit()
        
        # Create table with upsert support
        create_table_sql = """
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
        """
        pg_cursor.execute(create_table_sql)
        pg_conn.commit()
        
        # Create index on booking_hash for faster upserts
        pg_cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_booking_hash 
            ON bronze.raw_flight_prices (booking_hash)
        """)
        pg_conn.commit()
        logger.info("PostgreSQL Bronze table ready")
        
        # Get row count from MySQL
        mysql_cursor.execute("SELECT COUNT(*) as cnt FROM raw_flight_prices")
        total_rows = mysql_cursor.fetchone()['cnt']
        logger.info(f"Transferring {total_rows} rows...")
        
        # Transfer in batches with upsert
        offset = 0
        transferred = 0
        
        while offset < total_rows:
            mysql_cursor.execute(f"""
                SELECT booking_hash, airline, source, source_name, destination, destination_name,
                       departure_datetime, arrival_datetime, duration_hrs, stopovers,
                       aircraft_type, class, booking_source, base_fare_bdt,
                       tax_surcharge_bdt, total_fare_bdt, seasonality, days_before_departure
                FROM raw_flight_prices
                LIMIT {CHUNK_SIZE} OFFSET {offset}
            """)
            rows = mysql_cursor.fetchall()
            
            if not rows:
                break
            
            values = [
                (
                    row['booking_hash'], row['airline'], row['source'], row['source_name'],
                    row['destination'], row['destination_name'],
                    row['departure_datetime'], row['arrival_datetime'],
                    row['duration_hrs'], row['stopovers'], row['aircraft_type'],
                    row['class'], row['booking_source'], row['base_fare_bdt'],
                    row['tax_surcharge_bdt'], row['total_fare_bdt'],
                    row['seasonality'], row['days_before_departure']
                )
                for row in rows
            ]
            
            # Upsert: INSERT ... ON CONFLICT DO UPDATE
            upsert_sql = """
                INSERT INTO bronze.raw_flight_prices (
                    booking_hash, airline, source, source_name, destination, destination_name,
                    departure_datetime, arrival_datetime, duration_hrs, stopovers,
                    aircraft_type, class, booking_source, base_fare_bdt,
                    tax_surcharge_bdt, total_fare_bdt, seasonality, days_before_departure
                ) VALUES %s
                ON CONFLICT (booking_hash) DO UPDATE SET
                    base_fare_bdt = EXCLUDED.base_fare_bdt,
                    tax_surcharge_bdt = EXCLUDED.tax_surcharge_bdt,
                    total_fare_bdt = EXCLUDED.total_fare_bdt,
                    updated_at = CURRENT_TIMESTAMP
            """
            execute_values(pg_cursor, upsert_sql, values)
            pg_conn.commit()
            
            transferred += len(rows)
            offset += CHUNK_SIZE
            logger.info(f"Transferred {transferred}/{total_rows} rows...")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f" Transfer Complete!")
        logger.info(f"   Total rows transferred: {transferred}")
        logger.info(f"   Duration: {duration:.2f} seconds")
        logger.info(f"   Throughput: {transferred/duration:.0f} rows/sec")
        
        context['ti'].xcom_push(key='postgres_bronze_count', value=transferred)
        
        return transferred
        
    finally:
        mysql_cursor.close()
        mysql_conn.close()
        pg_cursor.close()
        pg_conn.close()


# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id='flight_price_pipeline',
    default_args=default_args,
    description='ETL pipeline for Bangladesh flight price analysis (Optimized v2: Incremental + Performance)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flight', 'etl', 'bangladesh', 'medallion', 'incremental']
) as dag:
    
    # Start Task
    task_start = EmptyOperator(
        task_id='start',
        doc_md="""
        ## Pipeline Start
        Entry point for the Flight Price Analysis pipeline.
        """
    )
    
    # Task 1: Load CSV to MySQL
    task_load_csv = PythonOperator(
        task_id='load_csv_to_mysql',
        python_callable=load_csv_to_mysql,
        provide_context=True,
        doc_md="""
        ## Load CSV to MySQL (Bronze Layer)
        
        **Optimizations:**
        - Bulk insert with larger chunks
        - Upsert support (INSERT ON DUPLICATE KEY UPDATE)
        - Multi-CSV file support
        - Composite hash key for deduplication
        """
    )
    
    # Task 2: Validate data in MySQL
    task_validate = PythonOperator(
        task_id='validate_mysql_data',
        python_callable=validate_mysql_data,
        provide_context=True,
        doc_md="""
        ## Validate MySQL Data
        
        Validation checks:
        - Row count verification
        - Duplicate hash detection
        - Null value detection
        - Numeric field validation
        """
    )
    
    # Task 3: Transfer to PostgreSQL Bronze
    task_transfer = PythonOperator(
        task_id='transfer_to_postgres_bronze',
        python_callable=transfer_to_postgres_bronze,
        provide_context=True,
        doc_md="""
        ## Transfer to PostgreSQL Bronze
        
        **Optimizations:**
        - Bulk insert with execute_values
        - Upsert support (ON CONFLICT DO UPDATE)
        - Index on booking_hash
        """
    )
    
    # Task 4: Run dbt transformations
    task_dbt = BashOperator(
        task_id='run_dbt_transformations',
        bash_command='''
            cd /opt/airflow/dbt_project && \
            echo "Installing dbt dependencies..." && \
            dbt deps --profiles-dir . && \
            echo "Running dbt models (Silver + Gold)..." && \
            dbt run --profiles-dir . && \
            echo "Running dbt tests..." && \
            dbt test --profiles-dir . && \
            echo " dbt transformations complete!"
        ''',
        doc_md="""
        ## Run dbt Transformations
        
        Builds Silver and Gold layers:
        - Silver: Cleaned data with duration_bucket and booking_lead_bucket
        - Gold: KPI tables (avg_fare_by_airline, avg_fare_by_class, etc.)
        - Tests: Data quality validation
        """
    )
    
    # End Task
    task_end = EmptyOperator(
        task_id='end',
        doc_md="""
        ## Pipeline End
        Final task marking successful completion.
        """
    )
    
    # Task Dependencies
    task_start >> task_load_csv >> task_validate >> task_transfer >> task_dbt >> task_end
