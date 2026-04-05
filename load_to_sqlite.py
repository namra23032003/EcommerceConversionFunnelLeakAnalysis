import sqlite3
import pandas as pd

CSV_PATH = "data/ecommerce_user_events.csv"
DB_PATH = "ecommerce_funnel.db"
TABLE_NAME = "ecommerce_user_events"

df = pd.read_csv(CSV_PATH)

conn = sqlite3.connect(DB_PATH)
df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

cursor = conn.cursor()
cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
row_count = cursor.fetchone()[0]

cursor.execute(f"SELECT COUNT(DISTINCT user_id) FROM {TABLE_NAME}")
user_count = cursor.fetchone()[0]

print("Data loaded successfully into SQLite.")
print(f"Database: {DB_PATH}")
print(f"Table: {TABLE_NAME}")
print(f"Rows loaded: {row_count}")
print(f"Distinct users: {user_count}")

conn.close()
