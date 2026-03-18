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
loc_a101 = pd.read_sql_query(
    "SELECT * FROM dw.loc_a101;",
    con=conn
)

print("Raw preview:")
print(loc_a101.head())
print("\nShape:", loc_a101.shape)

# 3. Make a copy
loc_trf = loc_a101.copy()

# --------------------------------------------------
# STEP 1 — Fix ID column
# Rule:
# remove leading/trailing spaces
# remove "-"
# Example: AW-00011000 -> AW00011000
# --------------------------------------------------
loc_trf["cid"] = loc_trf["cid"].astype(str).str.strip().str.replace("-", "", regex=False)

# --------------------------------------------------
# STEP 2 — Fix country column
# Rule:
# DE -> Germany
# US / Us / USA -> United States
# null / "" / blanks -> NA
# --------------------------------------------------
loc_trf["cntry"] = loc_trf["cntry"].fillna("").astype(str).str.strip()

loc_trf.loc[loc_trf["cntry"] == "", "cntry"] = "NA"
loc_trf.loc[loc_trf["cntry"].str.upper() == "DE", "cntry"] = "Germany"
loc_trf.loc[loc_trf["cntry"].str.upper().isin(["US", "USA"]), "cntry"] = "United States"

print("\nTransformed preview:")
print(loc_trf.head(10))

print("\nDistinct countries after transformation:")
print(loc_trf["cntry"].value_counts(dropna=False))

# 4. Create schema if not exists
cur.execute("CREATE SCHEMA IF NOT EXISTS trf;")
conn.commit()


# --------------------------------------------------
# 5. — TRUNCATE + LOAD
# --------------------------------------------------
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS trf.loc_a101"))

loc_trf.to_sql(
    name="loc_a101",
    con=engine,
    schema="trf",
    if_exists="append",
    index=False
)

print("\nCleaned data loaded into trf.loc_a101")

# 6. Close connections
cur.close()
conn.close()