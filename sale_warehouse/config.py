# config.py
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ----------------------------
# CSV Source Paths
# ----------------------------
DATA_DIR = os.path.join(BASE_DIR, "data")
CUSTOMER_CSV = os.path.join(DATA_DIR, "customer.csv")
CUSTOMER_LOCATION_CSV = os.path.join(DATA_DIR, "customer_location.csv")
CUSTOMER_INFO_CSV = os.path.join(DATA_DIR, "customer_info.csv")
PRODUCT_CATEGORIES_CSV = os.path.join(DATA_DIR, "product_categories.csv")
PRODUCT_INFO_CSV = os.path.join(DATA_DIR, "product_info.csv")
SALES_DETAILS_CSV = os.path.join(DATA_DIR, "sales_details.csv")

# ----------------------------
# SQL Server Connection
# ----------------------------
DB_SERVER = r"localhost\SQLEXPRESS01"  # raw string to handle backslash
DB_NAME = "sale_warehouse"
DB_DRIVER = "ODBC Driver 17 for SQL Server"
TRUSTED_CONNECTION = True  # Windows Auth

# ----------------------------
# Warehouse Tables
# ----------------------------
DIM_CUSTOMER_TABLE = "dim_customer"
DIM_PRODUCT_TABLE = "dim_product"
DIM_DATE_TABLE = "dim_date"
FACT_SALES_TABLE = "fact_sales"

# ----------------------------
# Incremental Tracker
# ----------------------------
TRACKER_FILE = os.path.join(BASE_DIR, "incremental_tracker.json")
