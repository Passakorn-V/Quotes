🧑‍💻 Data Engineer Test

A simple **ETL pipeline** and **data analysis workflow** using **Python**, **Apache Airflow**, and **Apache Spark**.

---

## ⚙️ Manual Run


### 1️⃣ Install dependencies
```bash
pip install -r requirements.txt
```

### 2️⃣ Scrape Raw Data

Generate example JSON files:
example_quotes.json and example_authors.json → saved in the data/ folder.

✅ Output:
data/example_quotes.json - data/example_authors.json

```bash

# Linux / macOS
python src/scrape_quotes.py

# Windows (PowerShell / CMD)
py src/scrape_quotes.py
```

### 3️⃣ Transform Data

Merge and clean scraped data into both CSV and Parquet formats.

✅ Output:
data/merged_data.csv - data/merged_data.parquet

```bash

# Linux / macOS
python src/transform_data.py

# Windows (PowerShell / CMD)
py src/transform_data.py
```

### 4️⃣ Test CSV vs Parquet

Verify data consistency between the two formats.

```bash

# Linux / macOS
python src/test.py

# Windows (PowerShell / CMD)
py src/test.py
```

### 5️⃣ Analyze with Spark

Run the Spark job to analyze the merged dataset and view results.

```bash

# Linux / macOS
python src/spark_analysis.py

# Windows (PowerShell / CMD)
py src/spark_analysis.py
```

### 🚀 Run with Airflow + Docker

Start Airflow locally using Docker Compose:

```bash

docker compose down -v
docker compose up airflow-init
docker compose up -d
```

### Then open your browser at:

```bash
http://localhost:8080
```

### Login credentials:

```bash
Username: admin
Password: admin
```



<img width="298" height="612" alt="image" src="https://github.com/user-attachments/assets/6c21c3a3-78eb-4217-8bb4-fa6865eb6111" />






