# transform.py
import pandas as pd
from .utils import generate_sk, logging

import pandas as pd
import logging

def transform_dim_customer(customer, customer_loc, customer_info, dim_customer_current=None):
    """
    Transform dim_customer by merging ERP and CRM sources,
    standardizing fields, fixing future dates, and applying SCD Type 2.

    Returns:
        df_new: new or changed rows with new surrogate keys
        dim_customer_current: updated existing dimension with expired rows marked
    """
    import pandas as pd
    import logging

    # -----------------------------
    # 1️⃣ Clean and standardize keys
    # -----------------------------
    customer_loc['CID'] = customer_loc['CID'].astype(str).str.replace('-', '', regex=False)
    customer_info['cst_key'] = customer_info['cst_key'].astype(str).str.strip()
    customer['CID'] = customer['CID'].astype(str).str.strip()

    # -----------------------------
    # 2️⃣ Merge sources
    # -----------------------------
    df = customer_info.merge(customer, left_on='cst_key', right_on='CID', how='left')
    df = df.merge(customer_loc, left_on='cst_key', right_on='CID', how='left')
    # -----------------------------
    # 3️⃣ Standardize fields
    # -----------------------------
    df['first_name'] = df['cst_firstname'].str.strip()
    df['last_name'] = df['cst_lastname'].str.strip()
    df['gender'] = df['cst_gndr'].str.upper()
    df['marital_status'] = df['cst_marital_status'].str.upper()

    df = df.rename(columns={
        'cst_id':'customer_id',
        'cst_key':'customer_key',
        'BDATE':'birth_date',
        'CNTRY':'country',
        'cst_create_date':'customer_create_date'
    })

    df = df[['customer_id','customer_key','first_name','last_name','gender',
             'marital_status','birth_date','country','customer_create_date']]
    
    # -----------------------------
    # 4️⃣ Convert to datetime and fix future dates
    # -----------------------------
    df['customer_create_date'] = pd.to_datetime(df['customer_create_date'], errors='coerce')
    today = pd.to_datetime("today").normalize()
    df.loc[df['customer_create_date'] > today, 'customer_create_date'] = today

    df = df.sort_values('customer_create_date').drop_duplicates(subset=['customer_key'], keep='last')
    # -----------------------------
    # 5️⃣ Apply SCD Type 2
    # -----------------------------
    df['effective_date'] = today
    df['end_date'] = pd.NaT
    df['current_flag'] = 'Y'

    if dim_customer_current is not None and not dim_customer_current.empty:
        tracked_cols = ['first_name','last_name','gender','marital_status','birth_date','country']

        # Merge with current dimension
        merged = df.merge(
            dim_customer_current[['customer_sk','customer_key'] + tracked_cols + ['current_flag']],
            on='customer_key', how='left', suffixes=('','_old')
        )
        # Row-wise change detection
        def is_changed(row):
            if pd.isna(row['customer_sk']):
                return True
            if row['current_flag_old'] != 'Y':
                return False
            for c in tracked_cols:
                old_val = row.get(c + '_old')
                new_val = row.get(c)

                # Normalize strings: strip and upper
                if isinstance(old_val, str):
                    old_val = old_val.strip().upper()
                if isinstance(new_val, str):
                    new_val = new_val.strip().upper()

                # Normalize datetimes: convert to datetime
                if 'date' in c.lower():
                    old_val = pd.to_datetime(old_val, errors='coerce')
                    new_val = pd.to_datetime(new_val, errors='coerce')

                # Compare NaNs as equal
                if pd.isna(old_val) and pd.isna(new_val):
                    continue
                if old_val != new_val:
                    return True
            return False

        merged['is_changed'] = merged.apply(is_changed, axis=1)
        # Mark old rows as expired
        expired_rows = merged[merged['is_changed'] & merged['customer_sk'].notna()]
        
        for _, row in expired_rows.iterrows():
            sk_old = row['customer_sk']
            dim_customer_current.loc[dim_customer_current['customer_sk'] == sk_old, 'end_date'] = today
            dim_customer_current.loc[dim_customer_current['customer_sk'] == sk_old, 'current_flag'] = 'N'
        

        new_rows = merged[merged['is_changed'] & merged['customer_sk'].isna()]
        merged['new'] = merged['is_changed'] & merged['customer_sk'].isna()

        




        df_new = merged[merged['is_changed']].copy()
        

        # Keep all original columns plus 'new'
        df_new = df_new[list(df.columns) + ['new']]
        


    else:
        # No existing dimension → all rows are new
        df['new'] = True
        df_new = df.copy()

    # -----------------------------
    # 6️⃣ Generate surrogate key for new rows
    # -----------------------------
    df_new = generate_sk(df_new, df_current=dim_customer_current, sk_col="customer_sk", prefix="CUST")
    df_new = df_new.rename(columns={'sk':'customer_sk'})
    

    logging.info(f"Transformed dim_customer: {len(df_new)} new/changed rows (SCD2 applied)")
    return df_new, dim_customer_current

def transform_dim_product(product_info, product_cat, dim_product_current=None):
    """
    Transform dim_product using SCD Type 2 logic.
    
    Returns:
        df_new: new or changed rows
        dim_product_current: updated existing dimension with expired rows
    """
    import pandas as pd
    import logging

    # -----------------------------
    # 1️ Derive category key
    # -----------------------------
    product_info['cat_id'] = (
        product_info['prd_key']
        .astype(str)
        .str.split('-', n=2)
        .str[:2]
        .str.join('_')
    )

    product_info['prd_start_dt'] = pd.to_datetime(product_info['prd_start_dt'], errors='coerce') # Generate end date using next start date per product 
    product_info = product_info.sort_values(['prd_key', 'prd_start_dt']) 
    product_info['prd_end_dt'] = product_info.groupby('prd_key')['prd_start_dt'].shift(-1)

    # -----------------------------
    # 2Merge sources
    # -----------------------------
    df = product_info.merge(
        product_cat,
        left_on='cat_id',
        right_on='ID',
        how='left'
    )

    # -----------------------------
    # 3️⃣ Standardize fields
    # -----------------------------
    df['product_name'] = df['prd_nm'].str.strip()
    df['product_line'] = df['prd_line'].str.upper()

    df = df.rename(columns={
        'prd_id': 'product_id',
        'prd_key': 'product_key',
        'CAT': 'category',
        'SUBCAT': 'subcategory',
        'MAINTENANCE': 'maintenance',
        'prd_cost': 'product_cost',
        'prd_start_dt': 'start_date',
        'prd_end_dt': 'end_date'
    })

    print(df.columns)

    df = df[
        [
            'product_id',
            'product_key',
            'product_name',
            'product_cost',
            'product_line',
            'category',
            'subcategory',
            'maintenance',
            'start_date',
            'end_date'
        ]
    ]


    # -----------------------------
    # 4️⃣ SCD Type 2 columns
    # -----------------------------
    today = pd.to_datetime("today").normalize()
    df['effective_date'] = today
    df['end_date_histroy'] = pd.NaT
    df['current_flag'] = 'Y'

    # -----------------------------
    # 5️⃣ Apply SCD Type 2 logic
    # -----------------------------
    if dim_product_current is not None and not dim_product_current.empty:

        tracked_cols = [
            'product_name',
            'product_cost',
            'product_line',
            'category',
            'subcategory',
            'maintenance',
            'start_date',
            'end_date'
        ]

        merged = df.merge(
        dim_product_current[
            ['product_sk', 'product_id'] + tracked_cols + ['current_flag']
        ],
        on='product_id',
        how='left',
        suffixes=('', '_old')
)

        # merged.rename(columns={'prd_cost':'product_cost'}, inplace=True)

        def is_changed(row):
        # NEW row → always True
            if pd.isna(row['product_sk']):
                return True

            # Skip non-current old rows
            if row.get('current_flag_old', 'Y') != 'Y':
                return False

            for c in tracked_cols:
                old_val = row.get(c + '_old')
                new_val = row.get(c)

                # Normalize strings
                if isinstance(old_val, str):
                    old_val = old_val.strip().upper()
                if isinstance(new_val, str):
                    new_val = new_val.strip().upper()

                # Normalize numbers
                if isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
                    old_val = float(old_val)
                    new_val = float(new_val)

                # Normalize datetimes
                if 'date' in c.lower():
                    old_val = pd.to_datetime(old_val, errors='coerce')
                    new_val = pd.to_datetime(new_val, errors='coerce')

                # Compare NaNs
                if pd.isna(old_val) and pd.isna(new_val):
                    continue
                if old_val != new_val:
                    return True

            return False


        merged['is_changed'] = merged.apply(is_changed, axis=1)
        # Expire old rows
        expired = merged[merged['is_changed'] & merged['product_sk'].notna()]
        print(expired)
        for _, row in expired.iterrows():
            sk_old = row['product_sk']
            
            print(dim_product_current[dim_product_current['product_sk'] == sk_old]['end_date_histroy'])

            dim_product_current.loc[
                dim_product_current['product_sk'] == sk_old, 'end_date_histroy'
            ] = today


            dim_product_current.loc[
                dim_product_current['product_sk'] == sk_old, 'current_flag'
            ] = 'N'

        # Identify new rows
        merged['new'] = merged['is_changed'] & merged['product_sk'].isna()

        df_new = merged[merged['is_changed']].copy()
        df_new = df_new[list(df.columns) + ['new']]

    else:
        # Initial load
        df['new'] = True
        df_new = df.copy()

    # -----------------------------
    # 6️⃣ Generate surrogate key
    # -----------------------------
    df_new = generate_sk(df_new, df_current=dim_product_current, sk_col="product_sk", prefix="PROD")
    df_new = df_new.rename(columns={'sk': 'product_sk'})

    logging.info(f"Transformed dim_product: {len(df_new)} new/changed rows (SCD2 applied)")
    return df_new, dim_product_current

def transform_dim_date(sales):
    dates = pd.concat([sales['sls_order_dt'], sales['sls_ship_dt'], sales['sls_due_dt']]).dropna().unique()
    df = pd.DataFrame(dates, columns=['full_date'])
    df['full_date'] = pd.to_datetime(df['full_date'], format='%Y%m%d', errors='coerce')
    df['day'] = df['full_date'].dt.day
    df['month'] = df['full_date'].dt.month
    df['month_name'] = df['full_date'].dt.month_name()
    df['quarter'] = df['full_date'].dt.quarter
    df['year'] = df['full_date'].dt.year

    df = generate_sk(df, prefix="DATE")
    df = df.rename(columns={'sk':'date_sk'})
    logging.info(f"Transformed dim_date with {len(df)} rows")
    return df

def transform_fact_sales(sales, dim_customer, dim_product, dim_date):
    """
    Transform fact_sales by mapping dimension surrogate keys and date keys.
    Handles key mismatches and ensures types are consistent.
    """

    # -----------------------------
    # 1️⃣ Standardize customer keys
    # -----------------------------
    sales['sls_cust_id'] = sales['sls_cust_id'].astype(str).str.strip()
    dim_customer['customer_key'] = dim_customer['customer_key'].astype(str).str.strip()

    # Add prefix if necessary (match dim_customer keys)
    if not sales['sls_cust_id'].str.startswith('AW').all():
        sales['sls_cust_id'] = 'AW' + sales['sls_cust_id'].str.zfill(8)

    # Merge customer_sk
    sales = sales.merge(
        dim_customer[['customer_sk','customer_key']],
        left_on='sls_cust_id',
        right_on='customer_key',
        how='left'
    )

    # -----------------------------
    # 2️⃣ Standardize product keys
    # -----------------------------

   

    sales['sls_prd_key'] = sales['sls_prd_key'].astype(str).str.strip()

    # Ensure string and strip spaces
    dim_product['product_key'] = dim_product['product_key'].astype(str).str.strip()

    # Remove first two parts of SKU
    dim_product['product_key'] = dim_product['product_key'].str.split('-').str[2:].str.join('-')

# Example:
# 'CO-RF-FR-R92B-58' -> ['CO','RF','FR','R92B','58'] -> take last 3 ['FR','R92B','58'] -> 'FR-R92B-58'


    # Merge product_sk
    sales = sales.merge(
        dim_product[['product_sk','product_key']],
        left_on='sls_prd_key',
        right_on='product_key',
        how='left'
    )

    # -----------------------------
    # 3️⃣ Convert dates safely
    # -----------------------------
    for col in ['sls_order_dt','sls_ship_dt','sls_due_dt']:
        sales[col] = pd.to_datetime(sales[col], format='%Y%m%d', errors='coerce')

    # -----------------------------
    # 4️⃣ Map date_sk from dim_date
    # -----------------------------
    date_map = dim_date[['date_sk','full_date']]

    sales = sales.merge(
        date_map, left_on='sls_order_dt', right_on='full_date', how='left'
    ).rename(columns={'date_sk':'order_date_sk'}).drop(columns=['full_date'])

    sales = sales.merge(
        date_map, left_on='sls_ship_dt', right_on='full_date', how='left'
    ).rename(columns={'date_sk':'ship_date_sk'}).drop(columns=['full_date'])

    sales = sales.merge(
        date_map, left_on='sls_due_dt', right_on='full_date', how='left'
    ).rename(columns={'date_sk':'due_date_sk'}).drop(columns=['full_date'])

    # -----------------------------
    # 5️⃣ Build final fact table
    # -----------------------------
    df = sales[[
        'sls_ord_num',
        'customer_sk',
        'product_sk',
        'order_date_sk',
        'ship_date_sk',
        'due_date_sk',
        'sls_quantity',
        'sls_price',
        'sls_sales'
    ]].copy()
    df['created_date'] = pd.to_datetime("today").normalize()

    # -----------------------------
    # 6️⃣ Generate surrogate key for fact_sales
    # -----------------------------
    df = generate_sk(df, prefix="SALES").rename(columns={'sk':'sales_sk'})

    logging.info(f"Transformed fact_sales with {len(df)} rows")
    return df