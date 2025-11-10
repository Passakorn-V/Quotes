import pandas as pd
import json
import re
import os

def transform_data(quotes_path, authors_path):
    
    # โหลดไฟล์ JSON
    with open(quotes_path, encoding="utf-8") as f:
        quotes = json.load(f)
    with open(authors_path, encoding="utf-8") as f:
        authors = json.load(f)

    # DataFrame
    quotes_df = pd.DataFrame(quotes)
    authors_df = pd.DataFrame(authors)

    # Clean คอลัมน์ birth_date และ birth_place ใน authors_df
    authors_df["birth_year"] = authors_df["birth_date"].apply(
        lambda x: re.findall(r"\d{4}", x)[0] if pd.notna(x) else None
    )
    authors_df["birth_country"] = authors_df["birth_place"].apply(
        lambda x: x.split(",")[-1].strip() if pd.notna(x) else None
    )
    authors_df["birth_country"] = authors_df["birth_country"].str.strip()

    # แก้ชื่อประเทศที่ไม่ตรงกัน
    authors_df["birth_country"] = authors_df["birth_country"].replace({
        'in The United States': 'The United States',
        'the Former Yugoslav Republic of': 'Former Yugoslavia',
        'Russian Federation': 'Russia'
    })

    # ปรับรูปแบบชื่อประเทศให้เป็น Title Case
    authors_df["birth_country"] = authors_df["birth_country"].str.title()

    # นับจำนวน quote ต่อ author
    quote_counts = quotes_df.groupby("author").size().reset_index(name="quote_count")

    # รวมข้อมูล authors กับ quote_counts
    merged = authors_df.merge(quote_counts, left_on="name", right_on="author", how="left")
    merged.drop(columns=["birth_date", "birth_place", "author"], inplace=True)
    merged["quote_count"] = merged["quote_count"].fillna(0).astype(int)

    # เรียงคอลัมน์ใหม่
    merged = merged[["name", "birth_year", "birth_country", "detail", "quote_count"]]

    # ตรวจสอบว่ามีโฟลเดอร์ data หรือไม่ ถ้าไม่มีก็สร้าง
    os.makedirs("data", exist_ok=True)

    # บันทึกไฟล์ CSV และ Parquet
    csv_path = "data/merged_data.csv"
    parquet_path = "data/merged_data.parquet"

    merged.to_csv(csv_path, index=False)
    merged.to_parquet(parquet_path, index=False)

    print(f" Transformed data saved to {csv_path} and {parquet_path} ")

    # return path สำหรับส่งต่อใน Airflow
    return csv_path, parquet_path


if __name__ == "__main__":
    # กรณีรันไฟล์นี้โดยตรง
    transform_data("data/example_quotes.json", "data/example_authors.json")
