# load.py
from .utils import get_engine, logging, load_tracker, save_tracker
from config import *
import pandas as pd
import os

# Create SQLAlchemy engine
engine = get_engine()

# Load or initialize tracker
tracker = load_tracker()

# -----------------------------
# Load dim_customer
# -----------------------------
def load_dim_customer(df_new, dim_customer_current=None):
    """
    Load dim_customer with incremental SCD2 logic using a tracker.
    """
    import logging
    import pandas as pd
    from sqlalchemy import text

    # Load last processed date from tracker
    last_date = tracker.get("dim_customer", "1900-01-01")
    last_date = pd.to_datetime(last_date)
    # Only keep rows that are new or changed after the last ETL run

    df_to_load = df_new[
        ((df_new['new'] == True) & (df_new['effective_date'] > last_date)) |
        (df_new['new'] == False)
          # new rows with date check                                          # existing rows, load all
    ]
    
    df_to_load = df_to_load.drop(columns=['new'], errors='ignore')

    # -----------------------------
    # 1️⃣ Expire old rows corresponding to these new rows
    # -----------------------------
    
    if dim_customer_current is not None and not dim_customer_current.empty:
        
        expired_rows = dim_customer_current[
            dim_customer_current['end_date'].notna() 
        ]
        if not expired_rows.empty:
            print('HEllo')
            sql = text(f"""
                UPDATE {DIM_CUSTOMER_TABLE}
                SET end_date = :end_date,
                    current_flag = 'N'
                WHERE customer_sk = :sk
            """)
            with engine.begin() as conn:
                for _, row in expired_rows.iterrows():
                    conn.execute(sql, {"end_date": row['end_date'], "sk": row['customer_sk']})
            logging.info(f"Marked {len(expired_rows)} rows as expired in {DIM_CUSTOMER_TABLE}")

    # -----------------------------
    # 2️⃣ Insert new rows
    # -----------------------------
    df_to_load.to_sql(DIM_CUSTOMER_TABLE, engine, if_exists='append', index=False)
    logging.info(f"Loaded {len(df_to_load)} new/changed rows into {DIM_CUSTOMER_TABLE}")

    # -----------------------------
    # 3️⃣ Update tracker
    # -----------------------------
    if( not df_to_load.empty):
        tracker["dim_customer"] = df_to_load['effective_date'].max().strftime("%Y-%m-%d")
        save_tracker(tracker)

# -----------------------------
# Load dim_product
# -----------------------------
def load_dim_product(df_new, dim_product_current=None):
    """
    Load dim_product with incremental SCD Type 2 logic using a tracker.
    """
    import logging
    import pandas as pd
    from sqlalchemy import text

    # -----------------------------
    # 0️⃣ Load tracker
    # -----------------------------
    last_date = tracker.get("dim_product", "1900-01-01")
    last_date = pd.to_datetime(last_date)

    # Load logic identical to dim_customer
    df_to_load = df_new[
        ((df_new['new'] == True) & (df_new['effective_date'] > last_date)) |
        (df_new['new'] == False)
    ]

    df_to_load = df_to_load.drop(columns=['new'], errors='ignore')

    # -----------------------------
    # 1️⃣ Expire old rows
    # -----------------------------
    if dim_product_current is not None and not dim_product_current.empty:

        expired_rows = dim_product_current[
            dim_product_current['end_date_histroy'].notna()
        ]

        if not expired_rows.empty:
            sql = text(f"""
                UPDATE {DIM_PRODUCT_TABLE}
                SET end_date_histroy = :end_date_histroy,
                    current_flag = 'N'
                WHERE product_sk = :sk
            """)

            with engine.begin() as conn:
                for _, row in expired_rows.iterrows():
                    conn.execute(
                        sql,
                        {"end_date_histroy": row['end_date_histroy'], "sk": row['product_sk']}
                    )

            logging.info(
                f"Marked {len(expired_rows)} rows as expired in {DIM_PRODUCT_TABLE}"
            )

    # -----------------------------
    # 2️⃣ Insert new rows
    # -----------------------------
    if not df_to_load.empty:
        df_to_load.to_sql(
            DIM_PRODUCT_TABLE,
            engine,
            if_exists='append',
            index=False
        )

        logging.info(
            f"Loaded {len(df_to_load)} new/changed rows into {DIM_PRODUCT_TABLE}"
        )

        # -----------------------------
        # 3️⃣ Update tracker
        # -----------------------------
        tracker["dim_product"] = (
            df_to_load['effective_date'].max().strftime("%Y-%m-%d")
        )
        save_tracker(tracker)
    else:
        logging.info(f"No new rows to load into {DIM_PRODUCT_TABLE}")

# Load dim_date
# -----------------------------
def load_dim_date(df):
    last_date = tracker.get("dim_date", "1900-01-01")
    last_date = pd.to_datetime(last_date)  # convert string back to datetime
    df_new = df[df['full_date'] > last_date]
    if len(df_new) > 0:
        df_new.to_sql(DIM_DATE_TABLE, engine, if_exists='append', index=False)
        tracker["dim_date"] = df_new['full_date'].max().strftime("%Y-%m-%d")
        save_tracker(tracker)
        logging.info(f"Loaded {len(df_new)} rows into {DIM_DATE_TABLE}")
    else:
        logging.info(f"No new rows to load into {DIM_DATE_TABLE}")

# -----------------------------
# Load fact_sales
# -----------------------------
def load_fact_sales(df):
    last_date = tracker.get("fact_sales", None)

    

    # Incremental load based on order_date_sk
    df_new = df[df['created_date'] > last_date] if last_date is not None else df

    if len(df_new) > 0:
        df_new.to_sql(FACT_SALES_TABLE, engine, if_exists='append', index=False)
        tracker["fact_sales"] = pd.to_datetime(df_new['created_date'].max()).strftime("%Y-%m-%d")
        save_tracker(tracker)
        logging.info(f"Loaded {len(df_new)} rows into {FACT_SALES_TABLE}")
    else:
        logging.info(f"No new rows to load into {FACT_SALES_TABLE}")
