from pyspark.sql import SparkSession
import os

# โฟลเดอร์ temp ใหม่
custom_temp_dir = os.path.abspath("C:/Users/User/Desktop/spark_temp")
os.makedirs(custom_temp_dir, exist_ok=True)

os.environ["SPARK_LOCAL_DIRS"] = custom_temp_dir
os.environ["HADOOP_OPTS"] = "-Djava.io.tmpdir=" + custom_temp_dir
os.environ["TMPDIR"] = custom_temp_dir
os.environ["TEMP"] = custom_temp_dir
os.environ["TMP"] = custom_temp_dir

spark = (
    SparkSession.builder
    .appName("QuoteAnalysis")
    .config("spark.local.dir", custom_temp_dir)
    .getOrCreate()
)

# โหลดข้อมูล
df = spark.read.parquet("data/merged_data.parquet")
df.createOrReplaceTempView("authors")

print("\n=== [Original DataFrame] ===")
df.show(5)
df.printSchema()

# ------------------------------
# [a] คนที่มี quote_count มากที่สุด
# ------------------------------
print("\n[a] คนที่มี quote_count มากที่สุด")
df_a = spark.sql("""
    SELECT 
        name,
        quote_count
    FROM authors
    ORDER BY quote_count DESC
    LIMIT 1;
""")
df_a.show()


# ------------------------------
# [b] คนที่แก่ที่สุด
# ------------------------------
print("\n[b] คนที่แก่ที่สุด มีใครบ้าง เกิดปีไหน")
df_b = spark.sql("""
    SELECT 
        name,
        birth_year
    FROM authors
    WHERE birth_year IS NOT NULL
    ORDER BY CAST(birth_year AS INT)
    LIMIT 1;
""")
df_b.show()


# ------------------------------
# [c] คนที่เกิดในประเทศเยอรมัน
# ------------------------------
print("\n[c] คนที่เกิดในประเทศเยอรมันมีทั้งหมดกี่คนและมี quote รวมทั้งหมดเท่าไหร่")
df_c = spark.sql("""
    SELECT 
        COUNT(*) AS total_authors,
        SUM(quote_count) AS total_quotes
    FROM authors
    WHERE birth_country = 'Germany';
""")
df_c.show()


# ------------------------------
# [ex] รายชื่อคนที่เกิดในเยอรมัน
# ------------------------------
print("\n[ex] คนที่เกิดในประเทศเยอรมันทั้งหมดมีใครบ้าง")
df_ex = spark.sql("""
    SELECT 
        name,
        birth_year,
        birth_country,
        quote_count
    FROM authors
    WHERE birth_country = 'Germany';
""")
df_ex.show()

# ถ้าอยากดู schema หรือแปลงเป็น Pandas ก็ทำได้
# df_ex.printSchema()
# print(df_ex.toPandas())
