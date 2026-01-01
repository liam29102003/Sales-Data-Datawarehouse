# main.py
from etl.extract import extract_all
from etl.transform import (
    transform_dim_customer,
    transform_dim_product,
    transform_dim_date,
    transform_fact_sales
)
from etl.load import (
    load_dim_customer,
    load_dim_product,
    load_dim_date,
    load_fact_sales
)
from etl.utils import (
    logging,
    get_dim_customer_current,
    get_dim_product_current
)

def run_etl():
    logging.info("ETL Started")

    # -------------------
    # 1️⃣ Extract
    # -------------------
    (
        customer,
        customer_loc,
        customer_info,
        product_cat,
        product_info,
        sales
    ) = extract_all()

    # -------------------
    # 2️⃣ Read current dimensions from warehouse
    # -------------------
    dim_customer_current = get_dim_customer_current()
    dim_product_current = get_dim_product_current()

    # -------------------
    # 3️⃣ Transform dimensions (SCD Type 2)
    # -------------------
    dim_customer_new, dim_customer_current = transform_dim_customer(
        customer,
        customer_loc,
        customer_info,
        dim_customer_current=dim_customer_current
    )

    dim_product_new, dim_product_current = transform_dim_product(
        product_info,
        product_cat,
        dim_product_current=dim_product_current
    )

    dim_date = transform_dim_date(sales)

    # -------------------
    # 4️⃣ Transform facts (use CURRENT dimension only)
    # -------------------
    fact_sales = transform_fact_sales(
        sales,
        dim_customer_current[dim_customer_current['current_flag'] == 'Y'],
        dim_product_current[dim_product_current['current_flag'] == 'Y'],
        dim_date
    )

    # -------------------
    # 5️⃣ Load
    # -------------------
    load_dim_customer(dim_customer_new, dim_customer_current)
    load_dim_product(dim_product_new, dim_product_current)
    load_dim_date(dim_date)
    load_fact_sales(fact_sales)

    logging.info("ETL Finished Successfully")

if __name__ == "__main__":
    run_etl()
