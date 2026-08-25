# Databricks notebook source
# MAGIC %md
# MAGIC # Anomaly Detection
# MAGIC Trains an Isolation Forest on the gold feature table (unsupervised --
# MAGIC `is_fraud` is used only afterward, to evaluate how well the anomaly
# MAGIC score lines up with the injected fraud labels). Writes scored
# MAGIC transactions to `fraudsentry.gold_scored_transactions`.
# MAGIC
# MAGIC Uses scikit-learn via a single-node `.toPandas()` since Community
# MAGIC Edition clusters are single-node anyway -- a real MLlib/distributed
# MAGIC model would be the natural next step on a paid workspace.

# COMMAND ----------

GOLD_TABLE = "fraudsentry.gold_account_features"
SCORED_TABLE = "fraudsentry.gold_scored_transactions"

FEATURE_COLS = [
    "amount",
    "hour_of_day",
    "rolling_avg_amount",
    "rolling_txn_count",
    "amount_deviation",
]

# COMMAND ----------

import pandas as pd
from sklearn.ensemble import IsolationForest

gold_pdf = spark.table(GOLD_TABLE).toPandas()

model = IsolationForest(
    n_estimators=200,
    contamination=0.02,  # matches the generator's default fraud rate
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
# MAGIC Sanity check only -- with a 2% contamination rate and simple
# MAGIC injected patterns, precision/recall in the 60-80% range is
# MAGIC expected and fine for a portfolio demo.

# COMMAND ----------

from sklearn.metrics import classification_report

print(classification_report(gold_pdf["is_fraud"], gold_pdf["predicted_fraud"]))

# COMMAND ----------

scored_df = spark.createDataFrame(gold_pdf)

(
    scored_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("dt")
    .saveAsTable(SCORED_TABLE)
)

print(f"wrote {scored_df.count()} rows to {SCORED_TABLE}")
