import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://postgres:1234@localhost:5432/dwh"
)

# Connect to DWH
conn = psycopg2.connect(
    host="localhost",
    database="dwh",
    user="postgres",
    password="1234",
    port="5432"
)

print("Connected to PostgreSQL")

# ----------------------------------------------
#  CURATED LAYER: CUSTOMER DIMENSION
# ----------------------------------------------

cust_info = pd.read_sql("SELECT * FROM trf.cust_info", conn)
cust_az12 = pd.read_sql("SELECT * FROM trf.cust_az12", conn)
loc_a101 = pd.read_sql("SELECT * FROM trf.loc_a101", conn)

print("Customer source data loaded")

df = cust_info.merge(
    cust_az12,
    how="left",
    left_on="cst_key",
    right_on="cid"
)

df = df.merge(
    loc_a101,
    how="left",
    left_on="cst_key",
    right_on="cid"
)

dim_customers = pd.DataFrame({
    "customer_id": df["cst_id"],
    "customer_number": df["cst_key"],
    "first_name": df["cst_firstname"],
    "last_name": df["cst_lastname"],
    "country": df["cntry"],
    "marital_status": df["cst_marital_status"],
    "gender": df["cst_gndr"],
    "birthdate": df["bdate"],
    "create_date": df["cst_create_date"]
})

dim_customers["birthdate"] = pd.to_datetime(dim_customers["birthdate"], errors="coerce").dt.date
dim_customers["create_date"] = pd.to_datetime(dim_customers["create_date"], errors="coerce").dt.date

dim_customers = dim_customers.sort_values(
    by=["customer_id"]
).reset_index(drop=True)

dim_customers.insert(0, "customer_key", dim_customers.index + 1)

print(dim_customers.head())

with engine.begin() as sql_conn:
    sql_conn.execute(text("CREATE SCHEMA IF NOT EXISTS crt;"))

create_customer_table_sql = """
CREATE TABLE IF NOT EXISTS crt.dim_customers (
    customer_key INT PRIMARY KEY,
    customer_id INT,
    customer_number VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    country VARCHAR(100),
    marital_status VARCHAR(50),
    gender VARCHAR(50),
    birthdate DATE,
    create_date DATE
);
"""

with engine.begin() as sql_conn:
    sql_conn.execute(text(create_customer_table_sql))
    sql_conn.execute(text("TRUNCATE TABLE crt.dim_customers;"))

dim_customers.to_sql(
    name="dim_customers",
    con=engine,
    schema="crt",
    if_exists="append",
    index=False
)

print("dim_customers loaded into crt.dim_customers successfully.")

# ----------------------------------------------
#  CURATED LAYER: PRODUCT DIMENSION
# ----------------------------------------------

prd_info = pd.read_sql("SELECT * FROM trf.prd_info", conn)
px_cat_g1v2 = pd.read_sql("SELECT * FROM trf.px_cat_g1v2", conn)

print("Product source data loaded")

df1 = prd_info.merge(
    px_cat_g1v2,
    how="left",
    left_on="cat_id",
    right_on="id"
)

dim_product = pd.DataFrame({
    "product_number": df1["prd_id"],
    "product_name": df1["prd_nm"],
    "cost": df1["prd_cost"],
    "product_line": df1["prd_line"],
    "start_date": df1["prd_start_dt"],
    "end_date": df1["prd_end_dt"],
    "category_id": df1["cat_id"],
    "category": df1["cat"],
    "subcategory": df1["subcat"],
    "maintenance": df1["maintenance"]
})

dim_product = dim_product.drop_duplicates()

dim_product["start_date"] = pd.to_datetime(dim_product["start_date"], errors="coerce").dt.date
dim_product["end_date"] = pd.to_datetime(dim_product["end_date"], errors="coerce").dt.date

dim_product = dim_product.sort_values(
    by=["product_number"]
).reset_index(drop=True)

dim_product.insert(0, "product_key", dim_product.index + 1)

print(dim_product.head())

create_product_table_sql = """
CREATE TABLE IF NOT EXISTS crt.dim_product (
    product_key INT PRIMARY KEY,
    product_number VARCHAR(50),
    product_name VARCHAR(100),
    cost DECIMAL(10, 2),
    product_line VARCHAR(100),
    start_date DATE,
    end_date DATE,
    category_id VARCHAR(50),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    maintenance VARCHAR(50)
);
"""

with engine.begin() as sql_conn:
    sql_conn.execute(text(create_product_table_sql))
    sql_conn.execute(text("TRUNCATE TABLE crt.dim_product;"))

dim_product.to_sql(
    name="dim_product",
    con=engine,
    schema="crt",
    if_exists="append",
    index=False
)

print("dim_product loaded into crt.dim_product successfully.")


# ----------------------------------------------
#  CURATED LAYER: FACT SALES
# ----------------------------------------------

sales = pd.read_sql("SELECT * FROM trf.sales_details", engine)

print("Sales source data loaded")
print(sales.head())

# Keep only current product versions
prd_info_current = prd_info[prd_info["prd_end_dt"].isna()].copy()


# Step 1: connect sales to current product version
fact_df = sales.merge(
    prd_info_current[["prd_key", "prd_id"]],
    how="left",
    left_on="sls_prd_key",
    right_on="prd_key"
)

# Step 2: connect to customer dimension
fact_df = fact_df.merge(
    dim_customers[["customer_key", "customer_id"]],
    how="left",
    left_on="sls_cust_id",
    right_on="customer_id"
)

# Step 3: connect to product dimension
fact_df = fact_df.merge(
    dim_product[["product_key", "product_number"]],
    how="left",
    left_on="prd_id",
    right_on="product_number"
)

# Build final fact table
fact_sales = pd.DataFrame({
    "order_number": fact_df["sls_ord_num"],
    "product_key": fact_df["product_key"],
    "customer_key": fact_df["customer_key"],
    "order_date": fact_df["sls_order_dt"],
    "ship_date": fact_df["sls_ship_dt"],
    "due_date": fact_df["sls_due_dt"],
    "sales_amount": fact_df["sls_sales"],
    "quantity": fact_df["sls_quantity"],
    "price": fact_df["sls_price"]
})

# Convert dates
fact_sales["order_date"] = pd.to_datetime(fact_sales["order_date"], errors="coerce").dt.date
fact_sales["ship_date"] = pd.to_datetime(fact_sales["ship_date"], errors="coerce").dt.date
fact_sales["due_date"] = pd.to_datetime(fact_sales["due_date"], errors="coerce").dt.date

print(fact_sales.head())
print(fact_sales.isna().sum())

print("sales rows:", len(sales))
print("fact_df rows after joins:", len(fact_df))
print("fact_sales rows:", len(fact_sales))

create_fact_table_sql = """
CREATE TABLE IF NOT EXISTS crt.fact_sales (
    order_number VARCHAR(50),
    product_key INT,
    customer_key INT,
    order_date DATE,
    ship_date DATE,
    due_date DATE,
    sales_amount DECIMAL(10, 2),
    quantity INT,
    price DECIMAL(10, 2)
);
"""

with engine.begin() as sql_conn:
    sql_conn.execute(text(create_fact_table_sql))
    sql_conn.execute(text("TRUNCATE TABLE crt.fact_sales;"))

fact_sales.to_sql(
    name="fact_sales",
    con=engine,
    schema="crt",
    if_exists="append",
    index=False
)

print("fact_sales loaded into crt.fact_sales successfully.")

print("sales rows:", len(sales))
print("fact_df rows after joins:", len(fact_df))
print("fact_sales rows:", len(fact_sales))


conn.close()