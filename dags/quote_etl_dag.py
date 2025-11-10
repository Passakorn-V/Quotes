import sys
import os
from datetime import datetime
from airflow import DAG
from airflow.decorators import task

# เพิ่ม path ของโปรเจกต์เข้าไป เพื่อให้ import src ได้
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.scrape_quotes import scrape_quotes
from src.transform_data import transform_data

with DAG(
    dag_id="quote_etl_dag",
    start_date=datetime(2025, 11, 9),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "quotes"]
) as dag:

    @task()
    def extract_task():
        print("🔹 Extracting data from quotes.toscrape.com ...")
        quotes_path, authors_path = scrape_quotes()
        print(f" Extracted: {quotes_path}, {authors_path}")
        return {"quotes_path": quotes_path, "authors_path": authors_path}

    @task()
    def transform_task(paths: dict):
        print("🔹 Transforming data ...")
        csv_path, parquet_path = transform_data(
            paths["quotes_path"], paths["authors_path"]
        )
        print(f" Transformed and saved to: {csv_path}, {parquet_path}")
        return {"csv_path": csv_path, "parquet_path": parquet_path}

    extract = extract_task()
    transform_task(extract)
