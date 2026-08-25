# Databricks notebook source
# MAGIC %md
# MAGIC # Anomaly Detection
# MAGIC Trains an Isolation Forest on the gold feature table (unsupervised --
# MAGIC `is_fraud` is used only afterward, to evaluate how well the anomaly
# MAGIC score lines up with the injected fraud labels). Writes scored
# MAGIC transactions to the SCORED_TABLE defined in `00_config`.
# MAGIC
# MAGIC Uses scikit-learn on a `.toPandas()` collection rather than
# MAGIC distributed MLlib -- at a few thousand rows the driver handles it
# MAGIC fine, and Isolation Forest has no direct MLlib equivalent. Moving to
# MAGIC a distributed model is the natural next step at real volume.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

FEATURE_COLS = [
    "amount",
    "hour_of_day",
    "rolling_avg_amount",
    "rolling_txn_count",
    "amount_deviation",
]

# COMMAND ----------

from sklearn.ensemble import IsolationForest

# Expected fraud prevalence. Isolation Forest uses this to set its
# decision threshold, so it should track how the data was generated:
# make_bronze_sample.py defaults to --fraud-rate 0.03, the streaming
# producer to 0.02. Tune here if you generated with something else.
CONTAMINATION = 0.03

gold_pdf = spark.table(GOLD_TABLE).toPandas()
print(f"scoring {len(gold_pdf)} rows; actual fraud rate in data: {gold_pdf['is_fraud'].mean():.2%}")

model = IsolationForest(
    n_estimators=200,
    contamination=CONTAMINATION,
    random_state=42,
)
model.fit(gold_pdf[FEATURE_COLS])

# COMMAND ----------

# decision_function: lower == more anomalous. Flip sign so higher == more
# suspicious, which reads more naturally as a "risk score".
gold_pdf["anomaly_score"] = -model.decision_function(gold_pdf[FEATURE_COLS])
gold_pdf["predicted_fraud"] = (model.predict(gold_pdf[FEATURE_COLS]) == -1).astype(int)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick evaluation against injected labels
# MAGIC Sanity check only. Don't expect strong numbers: the model sees no
# MAGIC labels, and two of the three injected fraud patterns (odd hour,
# MAGIC impossible travel) are only weakly represented in the current
# MAGIC feature set -- `hour_of_day` catches some of the first, and nothing
# MAGIC catches the second, since no geo-velocity feature exists yet.
# MAGIC Recall on the high-amount pattern should be much better than on
# MAGIC the other two. That gap is itself a good thing to talk about.

# COMMAND ----------

from sklearn.metrics import classification_report

print(classification_report(gold_pdf["is_fraud"], gold_pdf["predicted_fraud"]))

# COMMAND ----------

scored_df = spark.createDataFrame(gold_pdf)

(
    scored_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("dt")
    .saveAsTable(SCORED_TABLE)
)

print(f"wrote {scored_df.count()} rows to {SCORED_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Highest-risk transactions
# MAGIC The demo screenshot worth keeping: top anomaly scores, with the
# MAGIC true label alongside so you can see what it caught and what it
# MAGIC didn't.

# COMMAND ----------

from pyspark.sql import functions as F

display(
    spark.table(SCORED_TABLE)
    .orderBy(F.col("anomaly_score").desc())
    .select(
        "transaction_id", "account_id", "dt", "amount",
        "merchant_category", "hour_of_day", "amount_deviation",
        "anomaly_score", "predicted_fraud", "is_fraud",
    )
    .limit(25)
)
