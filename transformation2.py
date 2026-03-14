import pandas as pd
import psycopg2

# ---------------- CONNECT ----------------

conne = psycopg2.connect(
    host="localhost",
    database="dwh",
    user="postgres",
    password="1234",
    port="5432"
)

cursor = conne.cursor()

print("Connected to PostgreSQL")

# ---------------- LOAD SOURCE TABLE FROM RAW LAYER ----------------

prd_info = pd.read_sql("SELECT * FROM dw.prd_info", conne)

print("Data loaded into pandas DataFrame")

# ---------------- CHECK ORIGINAL ----------------

print("\n--- prd_info (original) ---")
print(prd_info.head())
print(prd_info.info())

# ---------------- MAKE A COPY FOR TRANSFORMATION ----------------

prd_info_trf = prd_info.copy()

print("\nCopy created for transformation")
print(prd_info_trf.head())

# ---------------- CLEANING ----------------

# 1. Convert data types
prd_info_trf["prd_cost"] = pd.to_numeric(prd_info_trf["prd_cost"], errors="coerce")
prd_info_trf["prd_start_dt"] = pd.to_datetime(prd_info_trf["prd_start_dt"], errors="coerce")

# 2. Set negative prd_cost values to 0
prd_info_trf.loc[prd_info_trf["prd_cost"] < 0, "prd_cost"] = 0

# 3. Split prd_key into cat_id and prd_key
prd_info_trf["cat_id"] = (
    prd_info_trf["prd_key"]
    .str.slice(0, 5)
    .str.replace("-", "_", regex=False)
)

prd_info_trf["prd_key"] = prd_info_trf["prd_key"].str.slice(6)

# 4. Sort rows before calculating prd_end_dt
prd_info_trf = prd_info_trf.sort_values(
    by=["prd_key", "prd_start_dt", "prd_id"],
    kind="mergesort",
    na_position="last"
)

# 5. Calculate prd_end_dt = next prd_start_dt - 1 day within same prd_key
next_start_dt = prd_info_trf.groupby("prd_key")["prd_start_dt"].shift(-1)
prd_info_trf["prd_end_dt"] = (next_start_dt - pd.Timedelta(days=1)).dt.date

# 6. Standardize prd_line values
prd_line_codes = prd_info_trf["prd_line"].apply(
    lambda value: value.strip().upper() if isinstance(value, str) else None
)

prd_info_trf["prd_line"] = prd_line_codes.map({
    "M": "Mountain",
    "R": "Road",
    "S": "Sport",
    "T": "Touring"
}).fillna("n/a")

# 7. Keep only date part in prd_start_dt
prd_info_trf["prd_start_dt"] = prd_info_trf["prd_start_dt"].dt.date

# ---------------- CHECK CLEANED DATA ----------------

print("\n--- prd_info_trf (cleaned) ---")
print(prd_info_trf.head())
print(prd_info_trf.info())
print(prd_info_trf[prd_info_trf["prd_cost"].isna()])

# ---------------- CREATE TABLE AND INSERT DATA----------------

# ---------------- CREATE TABLE ----------------

cursor.execute("CREATE SCHEMA IF NOT EXISTS trf;")
cursor.execute("DROP TABLE IF EXISTS trf.prd_info;")

create_table_sql = """
CREATE TABLE trf.prd_info (
  prd_id INT,
  cat_id TEXT,
  prd_key TEXT,
  prd_nm TEXT,
  prd_cost NUMERIC,
  prd_line TEXT,
  prd_start_dt DATE,
  prd_end_dt DATE
);
"""
cursor.execute(create_table_sql)

print("trf.prd_info recreated")

#Truncate target table before loading transformed data
cursor.execute("TRUNCATE TABLE trf.prd_info;")
print("trf.prd_info truncated")

#Reorder columns to match target table structure
prd_info_trf = prd_info_trf[
    [
        "prd_id",
        "cat_id",
        "prd_key",
        "prd_nm",
        "prd_cost",
        "prd_line",
        "prd_start_dt",
        "prd_end_dt"
    ]
]

#Convert DataFrame to CSV format in memory for bulk insert
from io import StringIO

buffer = StringIO()
prd_info_trf.to_csv(buffer, index=False, header=False)
buffer.seek(0)

#Use copy_expert for efficient bulk insert into target table
cursor.copy_expert(
    "COPY trf.prd_info FROM STDIN WITH CSV",
    buffer
)

conne.commit()