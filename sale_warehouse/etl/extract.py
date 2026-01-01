# extract.py
import pandas as pd
from config import *
import logging

def read_csv(file_path, **kwargs):
    try:
        df = pd.read_csv(file_path, **kwargs)
        logging.info(f"Read {file_path} with {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()

def extract_all():
    customer = read_csv(CUSTOMER_CSV)
    customer_loc = read_csv(CUSTOMER_LOCATION_CSV)
    customer_info = read_csv(CUSTOMER_INFO_CSV)
    product_cat = read_csv(PRODUCT_CATEGORIES_CSV)
    product_info = read_csv(PRODUCT_INFO_CSV)
    sales = read_csv(SALES_DETAILS_CSV)
    return customer, customer_loc, customer_info, product_cat, product_info, sales
