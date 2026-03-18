import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text

# 1. Connect
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="dwh",
    user="postgres",
    password="1234"
)
cur = conn.cursor()

engine = create_engine(
    "postgresql+psycopg2://postgres:1234@localhost:5432/dwh"
)

# 2. Read raw data from dw
df = pd.read_sql_query(
    "SELECT * FROM dw.sales_details;",
    con=engine
)

print("Raw preview:")
print(df.head())
print("\nShape:", df.shape)

# --------------------------------------------------
# STEP 1 — Fix invalid prices
# --------------------------------------------------
df["sls_price"] = pd.to_numeric(df["sls_price"], errors="coerce")
df["sls_quantity"] = pd.to_numeric(df["sls_quantity"], errors="coerce")
df["sls_sales"] = pd.to_numeric(df["sls_sales"], errors="coerce")

bad_price = df["sls_price"].isnull() | (df["sls_price"] <= 0)

df.loc[bad_price, "sls_price"] = (
    df.loc[bad_price, "sls_sales"] / df.loc[bad_price, "sls_quantity"]
)

# --------------------------------------------------
# STEP 2 — Recalculate sales
# --------------------------------------------------
df["sls_sales"] = df["sls_quantity"] * df["sls_price"]

# --------------------------------------------------
# STEP 3 — Fix order date (multi-line orders)
# --------------------------------------------------
order_counts = df.groupby("sls_ord_num")["sls_ord_num"].transform("count")

valid_order_dt = (
    df["sls_order_dt"].notnull() &
    (df["sls_order_dt"] != 0) &
    (df["sls_order_dt"].astype(str).str.len() == 8)
)

df["valid_order_dt"] = df["sls_order_dt"].where(valid_order_dt)

min_valid_order_dt = df.groupby("sls_ord_num")["valid_order_dt"].transform("max")

df.loc[order_counts > 1, "sls_order_dt"] = min_valid_order_dt[order_counts > 1]

# --------------------------------------------------
# STEP 4 — Fix single-line orders
# --------------------------------------------------
invalid_order_dt = (
    df["sls_order_dt"].isnull() |
    (df["sls_order_dt"] == 0) |
    (df["sls_order_dt"].astype(str).str.len() != 8)
)

single_bad_order_dt = (order_counts == 1) & invalid_order_dt

df.loc[single_bad_order_dt, "sls_order_dt"] = df.loc[single_bad_order_dt, "sls_ship_dt"]

# fallback
df["sls_order_dt"] = df["sls_order_dt"].fillna(df["sls_ship_dt"])

# --------------------------------------------------
# STEP 5 — Convert dates
# --------------------------------------------------
date_cols = ["sls_order_dt", "sls_ship_dt", "sls_due_dt"]

for col in date_cols:
    df[col] = pd.to_datetime(
        df[col].astype("Int64").astype(str),
        format="%Y%m%d",
        errors="coerce"
    )

# --------------------------------------------------
# STEP 6 — Round
# --------------------------------------------------
df["sls_price"] = df["sls_price"].round(2)
df["sls_sales"] = df["sls_sales"].round(2)

# --------------------------------------------------
# STEP 7 — Drop helper
# --------------------------------------------------
df = df.drop(columns=["valid_order_dt"])

print("\nTransformed preview:")
print(df.head(10))

# --------------------------------------------------
# STEP 8 — Create schema if not exists
# --------------------------------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS trf;")
conn.commit()

# --------------------------------------------------
# STEP 9 — TRUNCATE + LOAD
# --------------------------------------------------
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS trf.sales_details"))

df.to_sql(
    name="sales_details",
    con=engine,
    schema="trf",
    if_exists="append",
    index=False
)

print("\nCleaned data loaded into trf.sales_details")

# 10. Close
cur.close()
conn.close()