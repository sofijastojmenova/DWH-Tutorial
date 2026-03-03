#Connecting to the PostgreSQL database
import psycopg2
conne = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="1234",
        port="5432"
    )
print("✅ Connection successful!")

#Creating a cursor object to interact with the database and the database
conne.autocommit = True  # MUST be here

print("✅ Connection successful!")

cursor = conne.cursor()
print("✅ Cursor created successfully!")

cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'dwh';")
exists = cursor.fetchone()

if not exists:
    cursor.execute("CREATE DATABASE DWH;")
    print("✅ Database DWH created successfully!")
else:
    print("ℹ️ Database DWH already exists.")

#Connect to the DWH database
conne = psycopg2.connect(
    host="localhost",
    database="dwh",   # NOW connect to DWH
    user="postgres",
    password="1234",
    port="5432"
)

cursor = conne.cursor()
print("✅ DWH Connection successful!")

tables_sql = """
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.cust_info (
  cst_id BIGINT,
  cst_key TEXT,
  cst_firstname TEXT,
  cst_lastname TEXT,
  cst_marital_status TEXT,
  cst_gndr TEXT,
  cst_create_date TEXT
);

CREATE TABLE IF NOT EXISTS dw.cust_az12 (
  cid TEXT,
  bdate DATE,
  gen TEXT
);

CREATE TABLE IF NOT EXISTS dw.loc_a101 (
  cid TEXT,
  cntry TEXT
);

CREATE TABLE IF NOT EXISTS dw.prd_info (
  prd_id INT,
  prd_key TEXT,
  prd_nm TEXT,
  prd_cost NUMERIC,
  prd_line TEXT,
  prd_start_dt TEXT,
  prd_end_dt TEXT
);

CREATE TABLE IF NOT EXISTS dw.px_cat_g1v2 (
  id TEXT,
  cat TEXT,
  subcat TEXT,
  maintenance TEXT
);

CREATE TABLE IF NOT EXISTS dw.sales_details (
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
conne.commit()
print("✅ Schema dw + all 6 tables created successfully!")

# LOAD DATA USING COPY

# 1️⃣ CRM - Customer Info
with open("/Users/sofijastojmenova/Desktop/test/datasets/source_crm/cust_info.csv", "r") as f:
    cursor.copy_expert(
        "COPY dw.cust_info FROM STDIN WITH CSV HEADER DELIMITER ','",
        f
    )

# 2️⃣ ERP - Customer AZ12
with open("/Users/sofijastojmenova/Desktop/test/datasets/source_erp/CUST_AZ12.csv", "r") as f:
    cursor.copy_expert(
        "COPY dw.cust_az12 FROM STDIN WITH CSV HEADER DELIMITER ','",
        f
    )

# 3️⃣ ERP - Location
with open("/Users/sofijastojmenova/Desktop/test/datasets/source_erp/LOC_A101.csv", "r") as f:
    cursor.copy_expert(
        "COPY dw.loc_a101 FROM STDIN WITH CSV HEADER DELIMITER ','",
        f
    )

# 4️⃣ CRM - Product Info
with open("/Users/sofijastojmenova/Desktop/test/datasets/source_crm/prd_info.csv", "r") as f:
    cursor.copy_expert(
        "COPY dw.prd_info FROM STDIN WITH CSV HEADER DELIMITER ','",
        f
    )

# 5️⃣ ERP - Product Category
with open("/Users/sofijastojmenova/Desktop/test/datasets/source_erp/PX_CAT_G1V2.csv", "r") as f:
    cursor.copy_expert(
        "COPY dw.px_cat_g1v2 FROM STDIN WITH CSV HEADER DELIMITER ','",
        f
    )

# 6️⃣ CRM - Sales Details
with open("/Users/sofijastojmenova/Desktop/test/datasets/source_crm/sales_details.csv", "r") as f:
    cursor.copy_expert(
        "COPY dw.sales_details FROM STDIN WITH CSV HEADER DELIMITER ','",
        f
    )

conne.commit()

print("✅ All CSV files successfully imported!")

cursor.close()
conne.close()
print("✅ Done. Connection closed.")






