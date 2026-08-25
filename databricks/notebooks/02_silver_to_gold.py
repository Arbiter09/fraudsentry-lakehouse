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

# Trailing 10-transaction window for spend baselines.
account_window = Window.partitionBy("account_id").orderBy("timestamp").rowsBetween(-9, 0)

# Unbounded ordered window, for reaching back to the immediately previous
# transaction (lag) to compute travel distance and speed.
account_sequence = Window.partitionBy("account_id").orderBy("timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geo velocity
# MAGIC Implied speed between consecutive transactions on an account:
# MAGIC great-circle distance divided by elapsed time. A card used in two
# MAGIC distant cities minutes apart implies a physically impossible speed,
# MAGIC which is exactly the `impossible_travel` fraud pattern.
# MAGIC
# MAGIC Haversine is inlined here rather than imported -- this runs on the
# MAGIC Spark executors, and the Python copy in
# MAGIC `data_generator/generate_transactions.py` is the reference
# MAGIC implementation. Keep the two in sync.

# COMMAND ----------

EARTH_RADIUS_MILES = 3958.8

with_previous = (
    silver_df
    .withColumn("prev_lat", F.lag("lat").over(account_sequence))
    .withColumn("prev_lon", F.lag("lon").over(account_sequence))
    .withColumn("prev_timestamp", F.lag("timestamp").over(account_sequence))
)

phi1 = F.radians(F.col("prev_lat"))
phi2 = F.radians(F.col("lat"))
dphi = F.radians(F.col("lat") - F.col("prev_lat"))
dlambda = F.radians(F.col("lon") - F.col("prev_lon"))

haversine_a = (
    F.sin(dphi / 2) ** 2
    + F.cos(phi1) * F.cos(phi2) * F.sin(dlambda / 2) ** 2
)

with_geo = (
    with_previous
    .withColumn(
        "miles_from_previous",
        F.when(
            F.col("prev_lat").isNull(), F.lit(0.0)
        ).otherwise(2 * EARTH_RADIUS_MILES * F.asin(F.sqrt(haversine_a))),
    )
    .withColumn(
        "hours_since_previous",
        F.when(F.col("prev_timestamp").isNull(), F.lit(None).cast("double")).otherwise(
            (F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long")) / 3600.0
        ),
    )
    # First transaction per account has no predecessor -> 0, not null, so
    # the model doesn't have to handle missing values. Two transactions in
    # the same second would divide by zero, so floor the denominator.
    .withColumn(
        "implied_mph",
        F.when(
            F.col("hours_since_previous").isNull(), F.lit(0.0)
        ).otherwise(
            F.col("miles_from_previous") / F.greatest(F.col("hours_since_previous"), F.lit(1.0 / 60))
        ),
    )
)

# COMMAND ----------

gold_df = (
    with_geo
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
        "miles_from_previous",
        "hours_since_previous",
        "implied_mph",
        # Labels, kept for evaluation only -- never model features.
        "is_fraud",
        "fraud_pattern",
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does geo velocity separate the travel pattern?
# MAGIC `impossible_travel` should show a median implied speed in the
# MAGIC thousands of mph; everything else should sit near zero.

# COMMAND ----------

display(
    spark.table(GOLD_TABLE)
    .groupBy("fraud_pattern")
    .agg(
        F.count("*").alias("transactions"),
        F.round(F.median("implied_mph"), 1).alias("median_mph"),
        F.round(F.max("implied_mph"), 1).alias("max_mph"),
        F.round(F.median("miles_from_previous"), 1).alias("median_miles"),
    )
    .orderBy("fraud_pattern")
)
