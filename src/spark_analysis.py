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

# โหลดข้อมูลที่แปลงแล้ว
df = spark.read.parquet("data/merged_data.parquet")
df.createOrReplaceTempView("authors")


print("\n[a] คนที่มี quote_count มากที่สุด")
spark.sql("""
    SELECT 
        name,
        quote_count
    FROM authors
    ORDER BY quote_count DESC
    LIMIT 1;
""").show()


print("\n[b] คนที่แก่ที่สุด มีใครบ้าง เกิดปีไหน")
spark.sql("""
    SELECT 
        name,
        birth_year
    FROM authors
    WHERE birth_year IS NOT NULL
    ORDER BY CAST(birth_year AS INT)
    LIMIT 1;
""").show()


print("\n[c] คนที่เกิดในประเทศเยอรมันมีทั้งหมดกี่คนและมี quote รวมทั้งหมดเท่าไหร่")
spark.sql("""
    SELECT 
        COUNT(*) AS total_authors,
        SUM(quote_count) AS total_quotes
    FROM authors
    WHERE birth_country = 'Germany';
""").show()

print("\n[ex] คนที่เกิดในประเทศเยอรมันทั้งหมดมีใครบ้าง")
spark.sql("""
    SELECT 
        name,
        birth_year,
        birth_country,
        quote_count
    FROM authors
    WHERE birth_country = 'Germany';
""").show()
