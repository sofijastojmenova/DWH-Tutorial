import pandas as pd
import psycopg2
from io import StringIO

# Connect to DWH database
conne = psycopg2.connect(
    host="localhost",
    database="dwh",
    user="postgres",
    password="1234",
    port="5432"
)

cursor = conne.cursor()

print("Connected to PostgreSQL")

# 2. Read cust_info table from schema dw
cust_info = pd.read_sql("SELECT * FROM dw.cust_info", conne)


print("Data loaded into pandas DataFrames")

# 3. Check them
print("\n--- cust_info ---")
print(cust_info.head())
print(cust_info.info())

# ---------------- CLEANING ----------------

cust_info = cust_info.dropna(subset=["cst_key", "cst_id"])
cust_info = cust_info.drop_duplicates()

cust_info["cst_id"] = cust_info["cst_id"].astype("int64")

cust_info["cst_firstname"] = cust_info["cst_firstname"].str.strip()
cust_info["cst_lastname"] = cust_info["cst_lastname"].str.strip()

cust_info["cst_marital_status"] = cust_info["cst_marital_status"].fillna("N/A")
cust_info["cst_gndr"] = cust_info["cst_gndr"].fillna("N/A")


#Putting the cleaned data back to the database
from io import StringIO

# convert dataframe to CSV in memory
buffer = StringIO()
cust_info.to_csv(buffer, index=False, header=False)
buffer.seek(0)

# ---------------- CREATE TABLES ----------------

# copy dataframe into postgres table
tables_sql = """
CREATE SCHEMA IF NOT EXISTS trf;

CREATE TABLE IF NOT EXISTS trf.cust_info (
  cst_id BIGINT,
  cst_key TEXT,
  cst_firstname TEXT,
  cst_lastname TEXT,
  cst_marital_status TEXT,
  cst_gndr TEXT,
  cst_create_date TEXT
);

CREATE TABLE IF NOT EXISTS trf.cust_az12 (
  cid TEXT,
  bdate DATE,
  gen TEXT
);

CREATE TABLE IF NOT EXISTS trf.loc_a101 (
  cid TEXT,
  cntry TEXT
);

CREATE TABLE IF NOT EXISTS trf.prd_info (
  prd_id INT,
  prd_key TEXT,
  prd_nm TEXT,
  prd_cost NUMERIC,
  prd_line TEXT,
  prd_start_dt TEXT,
  prd_end_dt TEXT
);

CREATE TABLE IF NOT EXISTS trf.px_cat_g1v2 (
  id TEXT,
  cat TEXT,
  subcat TEXT,
  maintenance TEXT
);

CREATE TABLE IF NOT EXISTS trf.sales_details (
  sls_ord_num TEXT,
  sls_prd_key TEXT,
  sls_cust_id BIGINT,
  sls_order_dt INT,
  sls_ship_dt INT,
  sls_due_dt INT,
  sls_sales NUMERIC,
  sls_quantity INT,
  sls_price NUMERIC
);
"""
cursor.execute(tables_sql)

# ---------------- LOAD DATA ----------------

buffer = StringIO()
cust_info.to_csv(buffer, index=False, header=False)
buffer.seek(0)

cursor.copy_expert(
    "COPY trf.cust_info FROM STDIN WITH CSV",
    buffer
)

conne.commit()

print("Data loaded into transformation layer")

# ---------------- CLOSE ----------------

cursor.close()
conne.close()

print("Connection closed")