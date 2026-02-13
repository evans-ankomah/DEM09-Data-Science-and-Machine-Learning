"""
Data Loader Module
==================
Loads the ML-ready feature table with a cascading strategy:
  1. PostgreSQL Gold layer  (gold.ml_features)
  2. PostgreSQL Silver layer (silver.stg_flight_prices)
  3. PostgreSQL Bronze layer (bronze.raw_flight_prices)
  4. Local CSV fallback     (for local development)

Each layer is tried in order; if one fails or has no data, it
falls through to the next. This ensures ML training can always
find the best available data.
"""

import os
import logging
import glob
import hashlib
import pandas as pd

logger = logging.getLogger(__name__)

# Paths
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CSV_FOLDER = os.path.join(_PROJECT_ROOT, 'data')
DOCKER_CSV_FOLDER = '/opt/airflow/data'

# Columns that leak the target variable (total = base + tax)
LEAKAGE_COLUMNS = ['base_fare_bdt', 'tax_surcharge_bdt', 'base_fare', 'tax_surcharge']

# Target variable
TARGET = 'total_fare_bdt'


# ============================================
# PostgreSQL Layer Loaders
# ============================================

def _get_pg_connection():
    """Connect to PostgreSQL using environment variables."""
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'localhost'),
        port=int(os.environ.get('POSTGRES_PORT', 5432)),
        user=os.environ.get('POSTGRES_USER', 'postgres'),
        password=os.environ.get('POSTGRES_PASSWORD', 'postgres'),
        dbname=os.environ.get('POSTGRES_DATABASE', 'analytics'),
    )


def load_from_gold():
    """Load from gold.ml_features (best option: already feature-engineered)."""
    logger.info("Trying Gold layer: gold.ml_features...")
    try:
        conn = _get_pg_connection()
        df = pd.read_sql("SELECT * FROM gold.ml_features", conn)
        conn.close()
        if len(df) == 0:
            logger.warning("Gold layer exists but has 0 rows")
            return None
        logger.info(f"Loaded {len(df)} rows from Gold layer")
        return df
    except Exception as e:
        logger.warning(f"Gold layer unavailable: {e}")
        return None


def load_from_silver():
    """Load from silver.stg_flight_prices and apply feature engineering."""
    logger.info("Trying Silver layer: silver.stg_flight_prices...")
    try:
        conn = _get_pg_connection()
        df = pd.read_sql("SELECT * FROM silver.stg_flight_prices", conn)
        conn.close()
        if len(df) == 0:
            logger.warning("Silver layer exists but has 0 rows")
            return None
        logger.info(f"Loaded {len(df)} rows from Silver layer")
        return _engineer_features(df)
    except Exception as e:
        logger.warning(f"Silver layer unavailable: {e}")
        return None


def load_from_bronze():
    """Load from bronze.raw_flight_prices and apply cleaning + features."""
    logger.info("Trying Bronze layer: bronze.raw_flight_prices...")
    try:
        conn = _get_pg_connection()
        df = pd.read_sql("SELECT * FROM bronze.raw_flight_prices", conn)
        conn.close()
        if len(df) == 0:
            logger.warning("Bronze layer exists but has 0 rows")
            return None
        logger.info(f"Loaded {len(df)} rows from Bronze layer")
        return _engineer_features(df)
    except Exception as e:
        logger.warning(f"Bronze layer unavailable: {e}")
        return None


# ============================================
# CSV Fallback Loader
# ============================================

def load_from_csv():
    """Load from local CSV file (fallback for local development)."""
    logger.info("Trying CSV fallback...")

    csv_dirs = [CSV_FOLDER, DOCKER_CSV_FOLDER]
    csv_files = []
    for d in csv_dirs:
        csv_files.extend(glob.glob(os.path.join(d, 'Flight_Price_*.csv')))
        csv_files.extend(glob.glob(os.path.join(d, '*.csv')))

    if not csv_files:
        logger.error(f"No CSV files found in {csv_dirs}")
        return None

    csv_file = csv_files[0]
    logger.info(f"Loading from CSV: {csv_file}")
    df = pd.read_csv(csv_file)
    logger.info(f"Loaded {len(df)} rows from CSV")

    # Standardize column names to match dbt output
    rename_map = {
        'Airline': 'airline',
        'Source': 'source',
        'Source Name': 'source_name',
        'Destination': 'destination',
        'Destination Name': 'destination_name',
        'Departure Date & Time': 'departure_datetime',
        'Arrival Date & Time': 'arrival_datetime',
        'Duration (hrs)': 'duration_hrs',
        'Stopovers': 'stopovers',
        'Aircraft Type': 'aircraft_type',
        'Class': 'class',
        'Booking Source': 'booking_source',
        'Base Fare (BDT)': 'base_fare_bdt',
        'Tax & Surcharge (BDT)': 'tax_surcharge_bdt',
        'Total Fare (BDT)': 'total_fare_bdt',
        'Seasonality': 'seasonality',
        'Days Before Departure': 'days_before_departure',
    }
    df.rename(columns=rename_map, inplace=True)

    return _engineer_features(df)


def load_raw_data():
    """Load raw data without dropping leakage columns (for EDA only)."""
    csv_files = glob.glob(os.path.join(CSV_FOLDER, '*.csv'))
    if not csv_files:
        csv_files = glob.glob(os.path.join(DOCKER_CSV_FOLDER, '*.csv'))
    if not csv_files:
        raise FileNotFoundError("No CSV files found for raw data loading")

    df = pd.read_csv(csv_files[0])
    rename_map = {
        'Airline': 'airline', 'Source': 'source', 'Destination': 'destination',
        'Duration (hrs)': 'duration_hrs', 'Class': 'class',
        'Base Fare (BDT)': 'base_fare_bdt', 'Tax & Surcharge (BDT)': 'tax_surcharge_bdt',
        'Total Fare (BDT)': 'total_fare_bdt', 'Seasonality': 'seasonality',
        'Days Before Departure': 'days_before_departure',
        'Booking Source': 'booking_source', 'Aircraft Type': 'aircraft_type',
        'Stopovers': 'stopovers', 'Departure Date & Time': 'departure_datetime',
    }
    df.rename(columns=rename_map, inplace=True)
    return df


# ============================================
# Feature Engineering
# ============================================

def _engineer_features(df):
    """Apply feature engineering to match gold.ml_features output."""
    df = df.copy()

    # Parse departure datetime for time features
    if 'departure_datetime' in df.columns:
        dt = pd.to_datetime(df['departure_datetime'], errors='coerce')
        df['departure_month'] = dt.dt.month
        df['departure_weekday'] = dt.dt.dayofweek
        df['departure_hour'] = dt.dt.hour

    # Duration bucket
    if 'duration_hrs' in df.columns:
        df['duration_hrs'] = pd.to_numeric(df['duration_hrs'], errors='coerce')
        df['duration_bucket'] = pd.cut(
            df['duration_hrs'],
            bins=[0, 3, 6, float('inf')],
            labels=['Short (0-3h)', 'Medium (3-6h)', 'Long (6+h)'],
            right=True
        ).astype(str)

    # Booking lead bucket
    if 'days_before_departure' in df.columns:
        df['days_before_departure'] = pd.to_numeric(df['days_before_departure'], errors='coerce')
        df['booking_lead_bucket'] = pd.cut(
            df['days_before_departure'],
            bins=[0, 3, 14, 30, float('inf')],
            labels=[
                'Last Minute (0-3 days)',
                'Short Notice (4-14 days)',
                'Standard (15-30 days)',
                'Early Bird (30+ days)'
            ],
            right=True
        ).astype(str)

    # Drop leakage columns
    drop_cols = [c for c in LEAKAGE_COLUMNS if c in df.columns]
    if drop_cols:
        logger.info(f"Dropping leakage columns: {drop_cols}")
        df.drop(columns=drop_cols, inplace=True)

    # Drop non-feature columns
    drop_meta = [c for c in ['id', 'booking_hash', 'created_at', 'updated_at',
                              'source_name', 'destination_name',
                              'departure_datetime', 'arrival_datetime'] if c in df.columns]
    if drop_meta:
        df.drop(columns=drop_meta, inplace=True)

    # Drop NaN target rows
    if TARGET in df.columns:
        before = len(df)
        df.dropna(subset=[TARGET], inplace=True)
        dropped = before - len(df)
        if dropped > 0:
            logger.info(f"Dropped {dropped} rows with null target")

    return df


# ============================================
# Main Load Function
# ============================================

def load_data(source='auto'):
    """
    Load data using cascading strategy.

    Args:
        source: 'auto' (cascade Gold→Silver→Bronze→CSV),
                'postgres' (Gold only), or 'csv' (CSV only)

    Returns:
        pd.DataFrame with features ready for preprocessing
    """
    if source == 'csv':
        df = load_from_csv()
        if df is None:
            raise FileNotFoundError("No CSV data available")
        return df

    if source == 'postgres':
        df = load_from_gold()
        if df is not None:
            return df
        raise ConnectionError("Cannot load from PostgreSQL Gold layer")

    # Auto mode: cascade through all layers
    logger.info("Auto mode: cascading Gold → Silver → Bronze → CSV")

    for loader_name, loader_fn in [
        ('Gold',   load_from_gold),
        ('Silver', load_from_silver),
        ('Bronze', load_from_bronze),
        ('CSV',    load_from_csv),
    ]:
        df = loader_fn()
        if df is not None and len(df) > 0:
            logger.info(f"Successfully loaded data from {loader_name} layer")
            logger.info(f"Final shape: {df.shape}, columns: {list(df.columns)}")
            return df
        logger.info(f"{loader_name} layer: no data, trying next...")

    raise RuntimeError(
        "DATA LOAD FAILED: No data available from any source "
        "(Gold, Silver, Bronze, or CSV). Check your data pipeline."
    )
