🧑‍💻 Data Engineer Test

This project demonstrates a simple ETL pipeline and data analysis workflow using Python, Apache Airflow, and Apache Spark.

⚙️ Manual Run
1️⃣ Install dependencies
pip install -r requirements.txt

2️⃣ Scrape raw data

Generate example JSON data files (example_quotes.json, example_authors.json) in the data/ folder.

# Linux / macOS
python src/scrape_quotes.py

# Windows (PowerShell / CMD)
py src/scrape_quotes.py


✅ Output:

data/example_quotes.json

data/example_authors.json

3️⃣ Transform data

Merge and clean data into both CSV and Parquet formats.

# Linux / macOS
python src/transform_data.py

# Windows (PowerShell / CMD)
py src/transform_data.py


✅ Output:

data/merged_data.csv

data/merged_data.parquet

4️⃣ Test CSV vs Parquet

Verify data consistency between CSV and Parquet outputs.

# Linux / macOS
python src/test.py

# Windows (PowerShell / CMD)
py src/test.py

5️⃣ Analyze with Spark

Run Spark job to perform analysis and view results.

# Linux / macOS
python src/spark_analysis.py

# Windows (PowerShell / CMD)
py src/spark_analysis.py


🚀 Run with Airflow + Docker
Start Airflow locally
docker compose down -v
docker compose up airflow-init
docker compose up -d


Once Airflow is running, open your browser and go to:

👉 http://localhost:8080

Login credentials:

Username: admin
Password: admin

📁 Project Structure
project/
│
├── dags/
│   └── quote_etl_dag.py
│
├── data/
│   ├── example_quotes.json
│   ├── example_authors.json
│   ├── merged_data.csv
│   └── merged_data.parquet
│
├── sql/
│   ├── analysis_queries.sql
│   └── run_queries.py
│
├── src/
│   ├── scrape_quotes.py
│   ├── transform_data.py
│   ├── test.py
│   └── spark_analysis.py
│
├── requirements.txt
├── docker-compose.yml
└── README.md

🧠 Notes

Ensure Docker Desktop is running before starting Airflow.

You can modify DAGs and scripts to test your ETL logic.

Compatible with Python 3.9+.

💬 Example Workflow

Scrape → scrape_quotes.py

Transform → transform_data.py

Test → test.py

Analyze → spark_analysis.py

Automate → Airflow DAG

