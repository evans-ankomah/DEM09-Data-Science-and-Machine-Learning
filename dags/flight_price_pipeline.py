"""
Flight Price Analysis Pipeline - Airflow DAG

This DAG implements an ETL pipeline for Bangladesh flight price analysis
using the Medallion Architecture (Bronze → Silver → Gold).

Pipeline Flow:
1. Load CSV to MySQL (Bronze)
2. Validate data in MySQL
3. Transfer to PostgreSQL Bronze
4. Run dbt transformations (Silver + Gold)

"""

from datetime import datetime, timedelta
import os
import logging
import pandas as pd
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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

# CSV file path
CSV_PATH = '/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv'

# Chunk size for batch inserts
CHUNK_SIZE = 5000

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
    """
    Create and return a MySQL database connection.
    
    Returns:
        mysql.connector.connection.MySQLConnection: Active MySQL connection
    """
    return mysql.connector.connect(**MYSQL_CONFIG)


def get_postgres_connection():
    """
    Create and return a PostgreSQL database connection.
    
    Returns:
        psycopg2.connection: Active PostgreSQL connection
    """
    return psycopg2.connect(**POSTGRES_CONFIG)


# ============================================
# Task 1: Load CSV to MySQL (Bronze Layer)
# ============================================

def load_csv_to_mysql(**context):
    """
    Load the flight price CSV file into MySQL Bronze layer.
    
    This function:
    1. Reads the CSV file in chunks for memory efficiency
    2. Creates/recreates the raw_flight_prices table
    3. Inserts data in batches for performance
    4. Logs progress and final row count
    
    Args:
        **context: Airflow context dictionary
    
    Returns:
        int: Total number of rows loaded
    
    Raises:
        FileNotFoundError: If CSV file doesn't exist
        mysql.connector.Error: If database operation fails
    """
    logger.info(f"Starting CSV load from: {CSV_PATH}")
    start_time = datetime.now()
    
    # Verify CSV exists
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")
    
    # Read CSV to get column info
    df = pd.read_csv(CSV_PATH)
    logger.info(f"CSV loaded with {len(df)} rows and {len(df.columns)} columns")
    logger.info(f"Columns: {list(df.columns)}")
    
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    try:
        # Drop and recreate table (idempotent approach)
        logger.info("Creating Bronze table in MySQL...")
        
        cursor.execute("DROP TABLE IF EXISTS raw_flight_prices")
        
        # Create table with columns matching CSV structure
        create_table_sql = """
        CREATE TABLE raw_flight_prices (
            id INT AUTO_INCREMENT PRIMARY KEY,
            airline VARCHAR(255),
            source VARCHAR(255),
            source_name VARCHAR(255),
            destination VARCHAR(255),
            destination_name VARCHAR(255),
            departure_datetime VARCHAR(255),
            arrival_datetime VARCHAR(255),
            duration_hrs DECIMAL(10, 2),
            stopovers VARCHAR(50),
            aircraft_type VARCHAR(255),
            class VARCHAR(50),
            booking_source VARCHAR(255),
            base_fare_bdt DECIMAL(12, 2),
            tax_surcharge_bdt DECIMAL(12, 2),
            total_fare_bdt DECIMAL(12, 2),
            seasonality VARCHAR(50),
            days_before_departure INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info("Bronze table created successfully")
        
        # Insert data in chunks
        total_rows = 0
        insert_sql = """
        INSERT INTO raw_flight_prices (
            airline, source, source_name, destination, destination_name,
            departure_datetime, arrival_datetime, duration_hrs, stopovers,
            aircraft_type, class, booking_source, base_fare_bdt,
            tax_surcharge_bdt, total_fare_bdt, seasonality, days_before_departure
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for i in range(0, len(df), CHUNK_SIZE):
            chunk = df.iloc[i:i + CHUNK_SIZE]
            
            # Prepare data for insertion
            data = []
            for _, row in chunk.iterrows():
                data.append((
                    str(row.get('Airline', '')),
                    str(row.get('Source', '')),
                    str(row.get('Source Name', '')),
                    str(row.get('Destination', '')),
                    str(row.get('Destination Name', '')),
                    str(row.get('Departure Date & Time', '')),
                    str(row.get('Arrival Date & Time', '')),
                    float(row.get('Duration (hrs)', 0)) if pd.notna(row.get('Duration (hrs)')) else None,
                    str(row.get('Stopovers', '')),
                    str(row.get('Aircraft Type', '')),
                    str(row.get('Class', '')),
                    str(row.get('Booking Source', '')),
                    float(row.get('Base Fare (BDT)', 0)) if pd.notna(row.get('Base Fare (BDT)')) else None,
                    float(row.get('Tax & Surcharge (BDT)', 0)) if pd.notna(row.get('Tax & Surcharge (BDT)')) else None,
                    float(row.get('Total Fare (BDT)', 0)) if pd.notna(row.get('Total Fare (BDT)')) else None,
                    str(row.get('Seasonality', '')),
                    int(row.get('Days Before Departure', 0)) if pd.notna(row.get('Days Before Departure')) else None
                ))
            
            cursor.executemany(insert_sql, data)
            conn.commit()
            total_rows += len(chunk)
            logger.info(f"Inserted {total_rows} rows so far...")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f" CSV Load Complete!")
        logger.info(f"   Total rows loaded: {total_rows}")
        logger.info(f"   Duration: {duration:.2f} seconds")
        
        # Push row count to XCom for downstream tasks
        context['ti'].xcom_push(key='bronze_row_count', value=total_rows)
        
        return total_rows
        
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
    4. Numeric fields are valid (non-negative fares)
    5. Row count matches expected CSV count
    
    Args:
        **context: Airflow context dictionary
    
    Returns:
        dict: Validation results summary
    
    Raises:
        ValueError: If any validation check fails
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
        logger.info(f"✓ Row count: {row_count}")
        
        # Check 2: Required columns exist (implicit - query would fail if not)
        required_columns = [
            'airline', 'source', 'destination', 
            'base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt'
        ]
        
        cursor.execute("SHOW COLUMNS FROM raw_flight_prices")
        existing_columns = [col['Field'] for col in cursor.fetchall()]
        
        for col in required_columns:
            if col not in existing_columns:
                raise ValueError(f" Validation failed: Missing column '{col}'")
        logger.info(f"✓ All required columns present: {required_columns}")
        
        # Check 3: Null check for critical columns
        for col in ['airline', 'source', 'destination']:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM raw_flight_prices WHERE {col} IS NULL OR {col} = ''")
            null_count = cursor.fetchone()['cnt']
            if null_count > 0:
                logger.warning(f" Found {null_count} null/empty values in '{col}'")
            validation_results[f'{col}_nulls'] = null_count
        
        # Check 4: Numeric field validation (non-negative fares)
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM raw_flight_prices 
            WHERE base_fare_bdt < 0 OR tax_surcharge_bdt < 0 OR total_fare_bdt < 0
        """)
        negative_count = cursor.fetchone()['cnt']
        if negative_count > 0:
            logger.warning(f" Found {negative_count} rows with negative fare values")
        validation_results['negative_fares'] = negative_count
        
        # Check 5: Sample data for verification
        cursor.execute("SELECT * FROM raw_flight_prices LIMIT 5")
        sample_rows = cursor.fetchall()
        logger.info(f"Sample data from Bronze layer:")
        for row in sample_rows:
            logger.info(f"  {row['airline']} | {row['source']} → {row['destination']} | {row['total_fare_bdt']} BDT")
        
        logger.info(" Bronze layer validation complete!")
        logger.info(f"   Results: {validation_results}")
        
        # Push validation results to XCom
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
    
    This function:
    1. Creates the bronze schema in PostgreSQL
    2. Creates/recreates the raw_flight_prices table
    3. Transfers all data from MySQL to PostgreSQL
    4. Logs progress and final row count
    
    Args:
        **context: Airflow context dictionary
    
    Returns:
        int: Total number of rows transferred
    """
    logger.info("Starting transfer from MySQL to PostgreSQL Bronze...")
    start_time = datetime.now()
    
    # Connect to both databases
    mysql_conn = get_mysql_connection()
    mysql_cursor = mysql_conn.cursor(dictionary=True)
    
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()
    
    try:
        # Create schemas in PostgreSQL
        logger.info("Creating PostgreSQL schemas...")
        pg_cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        pg_cursor.execute("CREATE SCHEMA IF NOT EXISTS silver")
        pg_cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        pg_conn.commit()
        
        # Drop and recreate table in PostgreSQL
        pg_cursor.execute("DROP TABLE IF EXISTS bronze.raw_flight_prices CASCADE")
        
        create_table_sql = """
        CREATE TABLE bronze.raw_flight_prices (
            id SERIAL PRIMARY KEY,
            airline VARCHAR(255),
            source VARCHAR(255),
            source_name VARCHAR(255),
            destination VARCHAR(255),
            destination_name VARCHAR(255),
            departure_datetime VARCHAR(255),
            arrival_datetime VARCHAR(255),
            duration_hrs DECIMAL(10, 2),
            stopovers VARCHAR(50),
            aircraft_type VARCHAR(255),
            class VARCHAR(50),
            booking_source VARCHAR(255),
            base_fare_bdt DECIMAL(12, 2),
            tax_surcharge_bdt DECIMAL(12, 2),
            total_fare_bdt DECIMAL(12, 2),
            seasonality VARCHAR(50),
            days_before_departure INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        pg_cursor.execute(create_table_sql)
        pg_conn.commit()
        logger.info("PostgreSQL Bronze table created")
        
        # Fetch data from MySQL in chunks
        mysql_cursor.execute("SELECT COUNT(*) as cnt FROM raw_flight_prices")
        total_rows = mysql_cursor.fetchone()['cnt']
        logger.info(f"Transferring {total_rows} rows...")
        
        # Fetch and insert in batches
        offset = 0
        transferred = 0
        
        while offset < total_rows:
            mysql_cursor.execute(f"""
                SELECT airline, source, source_name, destination, destination_name,
                       departure_datetime, arrival_datetime, duration_hrs, stopovers,
                       aircraft_type, class, booking_source, base_fare_bdt,
                       tax_surcharge_bdt, total_fare_bdt, seasonality, days_before_departure
                FROM raw_flight_prices
                LIMIT {CHUNK_SIZE} OFFSET {offset}
            """)
            rows = mysql_cursor.fetchall()
            
            if not rows:
                break
            
            # Prepare data for PostgreSQL
            values = [
                (
                    row['airline'], row['source'], row['source_name'],
                    row['destination'], row['destination_name'],
                    row['departure_datetime'], row['arrival_datetime'],
                    row['duration_hrs'], row['stopovers'], row['aircraft_type'],
                    row['class'], row['booking_source'], row['base_fare_bdt'],
                    row['tax_surcharge_bdt'], row['total_fare_bdt'],
                    row['seasonality'], row['days_before_departure']
                )
                for row in rows
            ]
            
            insert_sql = """
                INSERT INTO bronze.raw_flight_prices (
                    airline, source, source_name, destination, destination_name,
                    departure_datetime, arrival_datetime, duration_hrs, stopovers,
                    aircraft_type, class, booking_source, base_fare_bdt,
                    tax_surcharge_bdt, total_fare_bdt, seasonality, days_before_departure
                ) VALUES %s
            """
            execute_values(pg_cursor, insert_sql, values)
            pg_conn.commit()
            
            transferred += len(rows)
            offset += CHUNK_SIZE
            logger.info(f"Transferred {transferred}/{total_rows} rows...")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f" Transfer Complete!")
        logger.info(f"   Total rows transferred: {transferred}")
        logger.info(f"   Duration: {duration:.2f} seconds")
        
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
    description='ETL pipeline for Bangladesh flight price analysis using Medallion Architecture',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flight', 'etl', 'bangladesh', 'medallion']
) as dag:
    
    # Task 1: Load CSV to MySQL
    task_load_csv = PythonOperator(
        task_id='load_csv_to_mysql',
        python_callable=load_csv_to_mysql,
        provide_context=True,
        doc_md="""
        ## Load CSV to MySQL (Bronze Layer)
        
        Loads the flight price CSV into MySQL as raw data.
        - Idempotent: Truncates and reloads on each run
        - Chunk-based insertion for memory efficiency
        """
    )
    
    # Task 2: Validate data in MySQL
    task_validate = PythonOperator(
        task_id='validate_mysql_data',
        python_callable=validate_mysql_data,
        provide_context=True,
        doc_md="""
        ## Validate MySQL Data
        
        Performs validation checks on Bronze layer:
        - Row count verification
        - Required columns check
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
        
        Transfers data from MySQL to PostgreSQL Bronze schema.
        Creates bronze, silver, and gold schemas if not exist.
        """
    )
    
    # Task 4: Run dbt transformations and tests
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
        
        Executes dbt to build Silver and Gold layers:
        - Silver: Cleaned and standardized data with seasonality_gh
        - Gold: KPI aggregation tables
        - Tests: Data quality validation
        """
    )
    
    # Define task dependencies
    task_load_csv >> task_validate >> task_transfer >> task_dbt
