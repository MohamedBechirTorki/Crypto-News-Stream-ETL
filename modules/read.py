import os
import psycopg2

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", 5432)
    )

conn = get_connection()
cur = conn.cursor()

# get last 5 rows (based on window_start)
cur.execute("""
    SELECT * 
    FROM window_features
    ORDER BY window_start DESC
    LIMIT 5;
""")

rows = cur.fetchall()

for row in rows:
    print(row)

conn.close()