# utils.py
import logging
import pandas as pd
from sqlalchemy import create_engine
from config import *
import os, json
import numpy as np


# ----------------------------
# Logging setup
# ----------------------------
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "etl.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------
# Surrogate key generator
# ----------------------------
def generate_sk(df_new, df_current=None, sk_col="sk", prefix="SK"):
    """
    Generate unique surrogate keys for new rows, dynamic for any dimension.

    Args:
        df_new: DataFrame of new/changed rows
        df_current: existing dimension table (to continue SK sequence)
        sk_col: name of surrogate key column in dimension
        prefix: optional prefix for SK

    Returns:
        df_new with new unique surrogate key column
    """
    df_new = df_new.copy()

    # Determine starting SK
    if df_current is not None and not df_current.empty and sk_col in df_current.columns:
        last_sk = df_current[sk_col].apply(lambda x: int(str(x).replace(prefix, ''))).max()
    else:
        last_sk = 0

    # Generate new SKs
    new_sks = range(last_sk + 1, last_sk + 1 + len(df_new))

    # Add prefix if needed
    if prefix:
        df_new[sk_col] = [f"{prefix}{x}" for x in new_sks]
    else:
        df_new[sk_col] = list(new_sks)

    return df_new

# ----------------------------
# SQLAlchemy Engine
# ----------------------------
def get_engine():
    
    db_url = f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}?driver={DB_DRIVER.replace(' ', '+')}&trusted_connection=yes"
   
    engine = create_engine(db_url, fast_executemany=True)
    return engine

# ----------------------------
# Incremental Tracker
# ----------------------------
def load_tracker():
    if os.path.exists(TRACKER_FILE):
        try:
            with open(TRACKER_FILE, "r") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            # File exists but empty or invalid
            return {}
    return {}


def save_tracker(tracker):
    # Convert any numpy types to native Python types
    tracker_serializable = {}
    for k, v in tracker.items():
        if isinstance(v, (np.integer, np.int64)):
            tracker_serializable[k] = int(v)
        elif isinstance(v, (np.floating, np.float64)):
            tracker_serializable[k] = float(v)
        else:
            tracker_serializable[k] = v
    with open(TRACKER_FILE, "w") as f:
        json.dump(tracker_serializable, f)

def get_dim_customer_current():
        """
        Reads the current dim_customer table from SQL Server.
        Returns a DataFrame, including historical rows.
        """
        query = f"SELECT * FROM {DIM_CUSTOMER_TABLE}"
        try:
            df = pd.read_sql(query, get_engine())
            return df
        except Exception as e:
            logging.warning(f"Could not read existing dim_customer: {e}")
            return pd.DataFrame()
        
def get_dim_product_current():
    """
    Reads the current dim_product table from the database.
    Returns a DataFrame, including historical rows.
    """
    query = f"SELECT * FROM {DIM_PRODUCT_TABLE}"
    try:
        df = pd.read_sql(query, get_engine())
        return df
    except Exception as e:
        logging.warning(f"Could not read existing dim_product: {e}")
        return pd.DataFrame()
