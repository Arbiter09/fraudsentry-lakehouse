# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline verification
# MAGIC Checks that 01-03 produced tables with sane contents, and returns a
# MAGIC JSON summary via `dbutils.notebook.exit()` so it can be read back
# MAGIC from a job run (see `databricks/deploy_and_run.py`).
# MAGIC
# MAGIC A job reporting SUCCESS only means nothing threw -- this is what
# MAGIC actually confirms the data is right.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json

from pyspark.sql import functions as F

summary = {}
problems = []

# COMMAND ----------

silver = spark.table(SILVER_TABLE)
gold = spark.table(GOLD_TABLE)
scored = spark.table(SCORED_TABLE)

summary["silver_rows"] = silver.count()
summary["gold_rows"] = gold.count()
summary["scored_rows"] = scored.count()

summary["distinct_days"] = silver.select("dt").distinct().count()
summary["distinct_accounts"] = silver.select("account_id").distinct().count()
summary["silver_unique_txn_ids"] = silver.select("transaction_id").distinct().count()

# COMMAND ----------

# Row counts must match across the three layers -- gold/scored are
# per-transaction, so a mismatch means rows were dropped or duplicated.
if not (summary["silver_rows"] == summary["gold_rows"] == summary["scored_rows"]):
    problems.append(
        f"row count mismatch: silver={summary['silver_rows']} "
        f"gold={summary['gold_rows']} scored={summary['scored_rows']}"
    )

if summary["silver_unique_txn_ids"] != summary["silver_rows"]:
    problems.append("duplicate transaction_ids survived the silver dedupe")

if summary["distinct_days"] < 2:
    problems.append(
        f"only {summary['distinct_days']} distinct day(s) -- timestamps weren't "
        "backdated, so the daily/rolling models have nothing to work with"
    )

# COMMAND ----------

# Rolling window should ramp 1..10 then flatten (rowsBetween(-9, 0)).
ramp = (
    gold.groupBy("rolling_txn_count").count().orderBy("rolling_txn_count").collect()
)
summary["rolling_counts_seen"] = [int(r["rolling_txn_count"]) for r in ramp]
if max(summary["rolling_counts_seen"]) < 10:
    problems.append(
        f"rolling_txn_count never reaches 10 (max="
        f"{max(summary['rolling_counts_seen'])}) -- not enough history per account"
    )

# COMMAND ----------

# Model sanity: did it actually separate anything, or flag everything/nothing?
scored_stats = scored.agg(
    F.avg("anomaly_score").alias("avg_score"),
    F.min("anomaly_score").alias("min_score"),
    F.max("anomaly_score").alias("max_score"),
    F.sum("predicted_fraud").alias("predicted_fraud"),
    F.sum("is_fraud").alias("actual_fraud"),
).collect()[0]

summary["avg_anomaly_score"] = round(float(scored_stats["avg_score"]), 4)
summary["min_anomaly_score"] = round(float(scored_stats["min_score"]), 4)
summary["max_anomaly_score"] = round(float(scored_stats["max_score"]), 4)
summary["predicted_fraud"] = int(scored_stats["predicted_fraud"])
summary["actual_fraud"] = int(scored_stats["actual_fraud"])

if summary["predicted_fraud"] == 0:
    problems.append("model flagged nothing -- contamination or features are wrong")
if summary["predicted_fraud"] == summary["scored_rows"]:
    problems.append("model flagged everything -- contamination or features are wrong")

# COMMAND ----------

# How well did the unsupervised score line up with the injected labels?
overlap = scored.filter((F.col("predicted_fraud") == 1) & (F.col("is_fraud") == 1)).count()
summary["true_positives"] = overlap
if summary["predicted_fraud"]:
    summary["precision"] = round(overlap / summary["predicted_fraud"], 3)
if summary["actual_fraud"]:
    summary["recall"] = round(overlap / summary["actual_fraud"], 3)

# COMMAND ----------

summary["problems"] = problems
summary["ok"] = not problems

print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
