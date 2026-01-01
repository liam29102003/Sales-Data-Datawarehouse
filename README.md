# Sale Warehouse ETL Project

## Overview

This is a **Python-based ETL pipeline** for a sales data warehouse built for learning and portfolio purposes. The pipeline ingests **ERP and CRM CSV files**, transforms them into dimensional models, and loads them into a **SQL Server warehouse**. It also implements **SCD Type 2** for tracking changes in the `dim_customer` dimension and supports incremental loads for facts.

---

## Features

- **Extract**: Reads multiple CSV files (Customer, Customer Location, Product Categories, Product Info, Sales) from ERP and CRM sources.
- **Transform**:
  - Cleans and standardizes keys and fields.
  - Generates surrogate keys for all dimensions and facts.
  - Applies **SCD Type 2** for `dim_customer` and `dim_product`.
  - Handles future dates and inconsistent data.
  - Converts sales dates to date keys for the warehouse.
- **Load**:
  - Loads dimensions and fact tables into **SQL Server**.
  - Tracks incremental loads using a JSON tracker file.
- **Warehouse Schema**:
  - **Dimensions**: `dim_customer`, `dim_product`, `dim_date`
  - **Fact**: `fact_sales`

---


---

## Data Sources

### ERP CSVs

- **Customer**: `CID`, `BDATE`, `GEN`
- **Customer Location**: `CID`, `CNTRY`
- **Product Categories**: `ID`, `CAT`, `SUBCAT`, `MAINTENANCE`

### CRM CSVs

- **Customer Info**: `cst_id`, `cst_key`, `cst_firstname`, `cst_lastname`, `cst_marital_status`, `cst_gndr`, `cst_create_date`
- **Product Info**: `prd_id`, `prd_key`, `prd_nm`, `prd_cost`, `prd_line`, `prd_start_dt`, `prd_end_dt`
- **Sales Details**: `sls_ord_num`, `sls_prd_key`, `sls_cust_id`, `sls_order_dt`, `sls_ship_dt`, `sls_due_dt`, `sls_sales`, `sls_quantity`, `sls_price`

---

## Dimension & Fact Modeling

### dim_customer

- Surrogate key: `customer_sk`
- Tracks historical changes using **SCD Type 2**
- Columns: `customer_id`, `customer_key`, `first_name`, `last_name`, `gender`, `marital_status`, `birth_date`, `country`, `customer_create_date`, `effective_date`, `end_date`, `current_flag`

### dim_product

- Surrogate key: `product_sk`
- - Tracks historical changes using **SCD Type 2**
- Columns: `product_id`, `product_key`, `product_name`, `prd_cost`, `product_line`, `category`, `subcategory`, `maintenance`, `start_date`, `end_date`, `effective_date`, `end_date_history`, `current_flag`

### dim_date

- Surrogate key: `date_sk`
- Columns: `full_date`, `year`, `month`, `day`, `weekday`

### fact_sales

- Surrogate key: `sales_sk`
- Foreign keys: `customer_sk`, `product_sk`, `order_date_sk`, `ship_date_sk`, `due_date_sk`
- Measures: `sls_quantity`, `sls_price`, `sls_sales`

---




