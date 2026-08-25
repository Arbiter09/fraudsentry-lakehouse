# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze -> Silver
# MAGIC Reads validated transaction JSON from the bronze layer, dedupes and
# MAGIC casts types, and writes a Delta silver table.
# MAGIC
# MAGIC Bronze location and table names come from `00_config` -- see that
# MAGIC notebook to switch between the UC Volume and an S3 external
# MAGIC location.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.read.json(BRONZE_PATH)
print(f"read {bronze_df.count()} raw records from {BRONZE_PATH}")
bronze_df.printSchema()

# COMMAND ----------

silver_df = (
    bronze_df
    .dropDuplicates(["transaction_id"])
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("ingested_at", F.to_timestamp("ingested_at"))
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("lat", F.col("lat").cast("double"))
    .withColumn("lon", F.col("lon").cast("double"))
    .withColumn("is_fraud", F.col("is_fraud").cast("int"))
    .filter(F.col("amount") > 0)
)

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("dt")
    .saveAsTable(SILVER_TABLE)
)

print(f"wrote {silver_df.count()} rows to {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check
# MAGIC Row count per day should show the ~14-day spread the sample builder
# MAGIC produces. A single row here means the generator wasn't backdated
# MAGIC (see `data_generator/make_bronze_sample.py --backdate-days`).

# COMMAND ----------

display(
    spark.table(SILVER_TABLE)
    .groupBy("dt")
    .agg(
        F.count("*").alias("transactions"),
        F.sum("is_fraud").alias("fraud"),
    )
    .orderBy("dt")
)
