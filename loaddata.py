import psycopg2

# Connect to DWH database
conne = psycopg2.connect(
    host="localhost",
    database="dwh",
    user="postgres",
    password="1234",
    port="5432"
)

cursor = conne.cursor()
print("✅ Connected to database: dwh")

# Optional but good practice
cursor.execute("SET search_path TO dw;")
print("✅ Using schema: dw")

# -----------------------------
# LOAD DATA USING COPY
# -----------------------------

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
print("✅ Connection closed.")