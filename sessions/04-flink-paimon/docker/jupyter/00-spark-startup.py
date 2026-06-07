# Auto-create a SparkSession when the "PySpark (DE5 Lakehouse)" kernel starts.
# Catalog config comes from PYSPARK_SUBMIT_ARGS in kernel.json, so students just
# pick this kernel and use `spark` / `sc` directly — no boilerplate needed.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("de5-pyspark-kernel").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext
print(f"✅ SparkSession ready (Spark {spark.version})")
print("   catalogs: paimon_lake (s3://paimon/warehouse), iceberg_lake (REST)")
print("   예) spark.sql('SELECT COUNT(*) FROM paimon_lake.bronze.ux_events_bronze').show()")
