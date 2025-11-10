import sqlite3
import pandas as pd

df = pd.read_csv("data/merged_data.csv")

conn = sqlite3.connect("quotes.db")

df.to_sql("authors", conn, if_exists="replace", index=False)

cursor = conn.cursor()

print("\n[a] คนที่มี quote_count มากที่สุด")
cursor.execute("""
    SELECT 
        name,
        quote_count
    FROM authors
    ORDER BY quote_count DESC
    LIMIT 1;
""")
print(cursor.fetchall())


print("\n[b] คนที่แก่ที่สุด มีใครบ้าง เกิดปีไหน")
cursor.execute("""
    SELECT 
        name,
        birth_year
    FROM authors
    WHERE birth_year IS NOT NULL
    ORDER BY CAST(birth_year AS INT)
    LIMIT 1;
""")
print(cursor.fetchall())


print("\n[c] คนที่เกิดในประเทศเยอรมันมีทั้งหมดกี่คนและมี quote รวมทั้งหมดเท่าไหร่")
cursor.execute("""
SELECT 
    COUNT(*) AS total_authors,
    SUM(quote_count) AS total_quotes
FROM authors
WHERE birth_country = 'Germany';
""")
print(cursor.fetchall())


print("\n[ex] คนที่เกิดในประเทศเยอรมันทั้งหมดมีใครบ้าง")
cursor.execute("""
    SELECT 
        name,
        birth_year,
        birth_country,
        quote_count
    FROM authors
    WHERE birth_country = 'Germany';
""")
print(cursor.fetchall())

conn.close()
