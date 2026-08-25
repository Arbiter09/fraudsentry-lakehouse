# databricks/ -- Community Edition notebooks

Run in order: `01_bronze_to_silver.py` -> `02_silver_to_gold.py` ->
`03_anomaly_detection.py`. Import each as a Databricks notebook (they're
plain `.py` files in Databricks' source format, so `File > Import` picks
them up directly and `git diff` stays readable, unlike `.dbc`/`.ipynb`).

## Community Edition setup notes

- **No instance profiles.** Set S3 read credentials on the cluster via
  Spark config instead:
  `fs.s3a.access.key` / `fs.s3a.secret.key` (use a scoped-down IAM user
  with read-only access to the bronze prefix -- never the account root
  keys).
- **Single-node clusters only.** `03_anomaly_detection.py` uses
  scikit-learn on a `.toPandas()` collection instead of distributed
  MLlib for this reason -- fine at this data volume.
- **Clusters auto-terminate on idle**, and job orchestration via the
  REST API is limited/unavailable on Community Edition. See the "Phase
  0 spike" note in the project plan: verify what your workspace actually
  allows before assuming Dagster can trigger these notebooks
  programmatically. If it can't, run them on Databricks' own notebook
  scheduler and have the Dagster asset represent that as a documented
  manual/external step.
