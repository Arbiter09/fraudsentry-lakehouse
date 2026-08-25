# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze -> Silver
# MAGIC Reads raw validated transaction JSON from S3 bronze, dedupes and
# MAGIC casts types, and writes a Delta silver table. Run this after the
# MAGIC Glue crawler has registered the bronze prefix (see infra/glue.tf).
# MAGIC
# MAGIC Community Edition note: this notebook expects AWS keys with
# MAGIC read-only S3 access set on the cluster (Admin Console -> instance
# MAGIC profile isn't available on Community Edition, so use
# MAGIC `spark.conf.set` with a scoped-down access key instead -- see
# MAGIC databricks/README.md).

# COMMAND ----------

BRONZE_PATH = "s3a://<bronze-bucket>/bronze/transactions/"
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
