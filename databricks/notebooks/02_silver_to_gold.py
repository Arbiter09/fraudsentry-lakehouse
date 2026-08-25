# Databricks notebook source
# MAGIC %md
# MAGIC # Silver -> Gold
# MAGIC Builds per-account behavioral features on a rolling window: this is
# MAGIC what the anomaly model in 03_anomaly_detection.py scores against.
# MAGIC Feature choices are deliberately simple (portfolio project, not a
# MAGIC production fraud model) but each one maps to a real fraud signal:
# MAGIC spend velocity, deviation from the account's own baseline, and
# MAGIC time-of-day.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

silver_df = spark.table(SILVER_TABLE)

account_window = Window.partitionBy("account_id").orderBy("timestamp").rowsBetween(-9, 0)

# COMMAND ----------

gold_df = (
    silver_df
    .withColumn("hour_of_day", F.hour("timestamp"))
    .withColumn("rolling_avg_amount", F.avg("amount").over(account_window))
    .withColumn("rolling_txn_count", F.count("transaction_id").over(account_window))
    .withColumn(
        "amount_deviation",
        F.col("amount") - F.col("rolling_avg_amount"),
    )
    .select(
        "transaction_id",
        "account_id",
        "timestamp",
        "dt",
        "amount",
        "merchant_category",
        "hour_of_day",
        "rolling_avg_amount",
        "rolling_txn_count",
        "amount_deviation",
        "is_fraud",  # kept for evaluation only, not used as a model feature
    )
)

# COMMAND ----------

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("dt")
    .saveAsTable(GOLD_TABLE)
)

print(f"wrote {gold_df.count()} rows to {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check
# MAGIC `rolling_txn_count` should ramp 1..10 as each account accumulates
# MAGIC history, then flatten at 10 (the window is `rowsBetween(-9, 0)`).
# MAGIC If it's 1 everywhere, the timestamps aren't spread across days.

# COMMAND ----------

display(
    spark.table(GOLD_TABLE)
    .groupBy("rolling_txn_count")
    .count()
    .orderBy("rolling_txn_count")
)
