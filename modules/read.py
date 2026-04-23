import sqlite3
import os

conn = sqlite3.connect("data/features.db")
cur = conn.cursor()

cur.execute("SELECT * FROM window_features;")
print(cur.fetchone())



conn.close()