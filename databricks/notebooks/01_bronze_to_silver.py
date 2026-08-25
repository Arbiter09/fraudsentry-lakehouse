# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze -> Silver
# MAGIC Reads raw validated transaction JSON from S3 bronze, dedupes and
# MAGIC casts types, and writes a Delta silver table. Run this after the
# MAGIC Glue crawler has registered the bronze prefix (see infra/glue.tf).
# MAGIC
# MAGIC ## Reading the bronze layer
# MAGIC On Databricks Free Edition, S3 access goes through a Unity Catalog
# MAGIC **storage credential + external location**, not the old
# MAGIC Community-Edition `fs.s3a.access.key` Spark config (CE was retired
# MAGIC 2026-01-01).
# MAGIC
# MAGIC If your Free Edition account can't create an external location,
# MAGIC use the UC Volume fallback instead: stage bronze files with
# MAGIC `databricks fs cp --recursive ./data/bronze <volume-path>` and set
# MAGIC `BRONZE_PATH` to the volume. See databricks/README.md.

# COMMAND ----------

# Option A -- UC external location over the S3 bronze prefix:
BRONZE_PATH = "s3://<bronze-bucket>/bronze/transactions/"
# Option B -- UC Volume fallback (see the note above):
# BRONZE_PATH = "/Volumes/<catalog>/<schema>/bronze/transactions/"

SILVER_TABLE = "fraudsentry.silver_transactions"

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.read.json(BRONZE_PATH)

# COMMAND ----------

silver_df = (
    bronze_df
    .dropDuplicates(["transaction_id"])
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("lat", F.col("lat").cast("double"))
    .withColumn("lon", F.col("lon").cast("double"))
    .withColumn("is_fraud", F.col("is_fraud").cast("int"))
    .filter(F.col("amount") > 0)
)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS fraudsentry")

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("dt")
    .saveAsTable(SILVER_TABLE)
)

print(f"wrote {silver_df.count()} rows to {SILVER_TABLE}")
