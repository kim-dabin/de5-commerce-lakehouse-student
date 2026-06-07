# Auto-create a SparkSession when the "PySpark (DE5 Lakehouse)" kernel starts.
# Catalog config comes from PYSPARK_SUBMIT_ARGS in kernel.json, so students just
# pick this kernel and use `spark` / `sc` directly — no boilerplate needed.
from pyspark.sql import SparkSession
import os
import pandas as pd
import pymysql

spark = SparkSession.builder.appName("de5-pyspark-kernel").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext


STARROCKS_HOST = os.getenv("STARROCKS_HOST", "starrocks-fe")
STARROCKS_PORT = int(os.getenv("STARROCKS_PORT", "9030"))
STARROCKS_USER = os.getenv("STARROCKS_USER", "root")
STARROCKS_PASSWORD = os.getenv("STARROCKS_PASSWORD", "")


def starrocks_connection(database=None):
    """Create a StarRocks MySQL-protocol connection for notebook queries."""
    return pymysql.connect(
        host=STARROCKS_HOST,
        port=STARROCKS_PORT,
        user=STARROCKS_USER,
        password=STARROCKS_PASSWORD,
        database=database,
        charset="utf8mb4",
        autocommit=True,
    )


def starrocks_sql(sql, database=None):
    """Run a StarRocks query and return a pandas DataFrame."""
    with starrocks_connection(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [column[0] for column in cursor.description or []]
            rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)


def starrocks_execute(sql, database=None):
    """Run a StarRocks statement and return affected row count."""
    with starrocks_connection(database=database) as conn:
        with conn.cursor() as cursor:
            affected = cursor.execute(sql)
    return affected


sr_sql = starrocks_sql
sr_execute = starrocks_execute

print(f"✅ SparkSession ready (Spark {spark.version})")
print("   catalogs: paimon_lake (s3://paimon/warehouse), iceberg_lake (REST)")
print("   예) spark.sql('SELECT COUNT(*) FROM paimon_lake.bronze.ux_events_bronze').show()")
print(f"✅ StarRocks helper ready ({STARROCKS_HOST}:{STARROCKS_PORT}, user={STARROCKS_USER})")
print("   예) starrocks_sql('SHOW CATALOGS')")
