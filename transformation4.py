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

# 2. Read source table from dw
cust_az12 = pd.read_sql_query(
    "SELECT * FROM dw.cust_az12;",
    con=engine
)

print("Raw preview:")
print(cust_az12.head())
print("\nShape:", cust_az12.shape)

# 3. Make a copy
cust_trf = cust_az12.copy()

# --------------------------------------------------
# STEP 1 — Fix customer ID
# Rule:
# remove first 3 letters "NAS"
# Example: NASAW00011000 -> AW00011000
# --------------------------------------------------
cust_trf["cid"] = cust_trf["cid"].astype(str).str.strip()
cust_trf["cid"] = cust_trf["cid"].str.replace(r"^NAS", "", regex=True)

# --------------------------------------------------
# STEP 2 — Fix gender values
# Rule:
# nulls / empty strings / blanks -> "NA"
# F / female -> "Female"
# M / male -> "Male"
# --------------------------------------------------
cust_trf["gen"] = cust_trf["gen"].fillna("").astype(str).str.strip().str.lower()

cust_trf["gen"] = cust_trf["gen"].replace({
    "": "NA",
    "f": "Female",
    "female": "Female",
    "m": "Male",
    "male": "Male"
})

# if anything unexpected remains, also set to NA
cust_trf.loc[~cust_trf["gen"].isin(["Female", "Male", "NA"]), "gen"] = "NA"

# --------------------------------------------------
# STEP 3 — Fix birth date
# Rule:
# convert to datetime
# if bdate > current date -> NULL
# --------------------------------------------------
cust_trf["bdate"] = pd.to_datetime(cust_trf["bdate"], errors="coerce")

today = pd.Timestamp.today().normalize()
cust_trf.loc[cust_trf["bdate"] > today, "bdate"] = None

print("\nTransformed preview:")
print(cust_trf.head(10))

print("\nNull values after transformation:")
print(cust_trf.isnull().sum())

# 4. Create schema if not exists
cur.execute("CREATE SCHEMA IF NOT EXISTS trf;")
conn.commit()

# --------------------------------------------------
# 5. — TRUNCATE + LOAD
# --------------------------------------------------
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS trf.cust_az12"))

cust_trf.to_sql(
    name="cust_az12",
    con=engine,
    schema="trf",
    if_exists="append",
    index=False
)

print("\nCleaned data loaded into trf.cust_az12")

# 6. Close connections
cur.close()
conn.close()