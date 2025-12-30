# Source-to-Target Mapping (STTM)

This document maps the source CSV files from ERP and CRM to the target data warehouse tables (`dim_customer`, `dim_product`, `dim_date`, `fact_sales`).  
It also describes the transformations needed for each column.

---

## dim_customer

| Target Column           | Source File(s)                     | Source Column       | Transformation / Notes                           |
|-------------------------|----------------------------------|-------------------|------------------------------------------------|
| customer_sk             | ETL                               | -                 | Surrogate key (generated)                      |
| customer_id             | customer_info.csv                 | cst_id            | Direct mapping                                 |
| customer_key            | customer_info.csv                 | cst_key           | Direct mapping                                 |
| first_name              | customer_info.csv                 | cst_firstname     | Strip whitespace                               |
| last_name               | customer_info.csv                 | cst_lastname      | Strip whitespace                               |
| gender                  | customer_info.csv                 | cst_gndr          | Standardize to 'M'/'F'                         |
| marital_status          | customer_info.csv                 | cst_marital_status| Standardize 'S'/'M'                            |
| birth_date              | customer.csv                      | BDATE             | Keep as string or parse if needed             |
| country                 | customer_location.csv             | CNTRY             | Join on CID / cst_key                          |
| customer_create_date    | customer_info.csv                 | cst_create_date   | Keep as date                                   |
| source_system           | customer_info.csv + customer.csv  | -                 | Mark source as ERP/CRM                         |

---

## dim_product

| Target Column           | Source File(s)                     | Source Column       | Transformation / Notes                          |
|-------------------------|----------------------------------|-------------------|------------------------------------------------|
| product_sk              | ETL                               | -                 | Surrogate key (generated)                      |
| product_id              | product_info.csv                  | prd_id            | Direct mapping                                 |
| product_key             | product_info.csv                  | prd_key           | Direct mapping                                 |
| product_name            | product_info.csv                  | prd_nm            | Clean text                                     |
| product_cost            | product_info.csv                  | prd_cost          | Convert to numeric                             |
| product_line            | product_info.csv                  | prd_line          | Standardize text                               |
| category                | product_categories.csv            | CAT               | Join on prd_key or ID                          |
| subcategory             | product_categories.csv            | SUBCAT            | Join on prd_key or ID                          |
| maintenance             | product_categories.csv            | MAINTENANCE       | Map 'Yes'/'No' to boolean                      |
| start_date              | product_info.csv                  | prd_start_dt      | Parse to date                                  |
| end_date                | product_info.csv                  | prd_end_dt        | Parse to date if exists                         |
| source_system           | CRM + ERP                         | -                 | Mark source                                    |

---

## dim_date

| Target Column           | Source File(s)                     | Source Column         | Transformation / Notes                         |
|-------------------------|----------------------------------|-------------------|------------------------------------------------|
| date_sk                 | ETL                               | -                 | Surrogate key (generated)                      |
| full_date               | sales_details.csv                 | sls_order_dt / sls_ship_dt / sls_due_dt | Convert numeric YYYYMMDD to date          |
| day                     | Derived                           | -                 | Extract from full_date                          |
| month                   | Derived                           | -                 | Extract from full_date                          |
| month_name              | Derived                           | -                 | Extract from full_date                          |
| quarter                 | Derived                           | -                 | Extract from full_date                          |
| year                    | Derived                           | -                 | Extract from full_date                          |

---

## fact_sales

| Target Column           | Source File(s)                     | Source Column       | Transformation / Notes                          |
|-------------------------|----------------------------------|-------------------|------------------------------------------------|
| sales_sk                | ETL                               | -                 | Surrogate key (generated)                      |
| order_number            | sales_details.csv                 | sls_ord_num       | Direct mapping                                 |
| customer_sk             | dim_customer                      | -                 | Lookup by customer_key                          |
| product_sk              | dim_product                       | -                 | Lookup by product_key                            |
| order_date_sk           | dim_date                          | sls_order_dt      | Convert YYYYMMDD → date_sk                       |
| ship_date_sk            | dim_date                          | sls_ship_dt       | Convert YYYYMMDD → date_sk                       |
| due_date_sk             | dim_date                          | sls_due_dt        | Convert YYYYMMDD → date_sk                       |
| quantity                | sales_details.csv                 | sls_quantity      | Numeric                                        |
| unit_price              | sales_details.csv                 | sls_price         | Numeric                                        |
| sales_amount            | sales_details.csv                 | sls_sales         | Numeric                                        |
| source_system           | sales_details.csv                 | -                 | Mark CRM                                       |

---

## Notes

1. **Surrogate keys** (`*_sk`) are generated in ETL; not present in source files.  
2. **Joins required**:
   - `dim_customer`: customer_info.csv + customer.csv + customer_location.csv (on CID/cst_key)  
   - `dim_product`: product_info.csv + product_categories.csv  
3. **Incremental load**:
   - `dim_customer` → by `customer_create_date`  
   - `dim_product` → by `prd_start_dt`  
   - `fact_sales` → by `sls_order_dt`  

