# databricks/ -- Free Edition notebooks

Run in order: `01_bronze_to_silver.py` -> `02_silver_to_gold.py` ->
`03_anomaly_detection.py`. Import each as a Databricks notebook (they're
plain `.py` files in Databricks' source format, so `File > Import` picks
them up directly and `git diff` stays readable, unlike `.dbc`/`.ipynb`).

## Which Databricks?

**Databricks Community Edition was retired on 2026-01-01 and no longer
exists.** Sign up for [Databricks Free
Edition](https://www.databricks.com/learn/free-edition) instead -- it
replaced CE and is materially more capable.

## Free Edition: what it gives us

- **Unity Catalog** (one metastore per account). This is a real upgrade
  over the old CE plan: UC tracks table- and column-level lineage
  natively, so the governance story no longer depends entirely on
  self-hosted DataHub. See `governance/README.md`.
- **Jobs / Workflows**, capped at 5 concurrent job tasks per account.
  CE effectively had no usable Jobs API, which is why
  `dagster/fraudsentry_dagster/databricks_asset.py` was written
  defensively. On Free Edition that asset has a real chance of working
  as intended.
- **Serverless compute**, so no cluster config or idle-termination
  babysitting.

## Free Edition: what it costs us

- **Serverless-only, quota-limited.** One SQL warehouse, `2X-Small`
  only. Fine at this project's data volume.
- **Python and SQL only** -- no Scala or R. Doesn't affect this repo.
- **No account console or account-level APIs.** Workspace-level APIs
  (including Jobs) are what we need, so this should be fine, but it's
  the reason storage-credential setup below is uncertain.
- **Not licensed for commercial use.** Portfolio/learning only -- which
  is exactly this project.

## Open question: reading the S3 bronze layer

The notebooks currently read `s3a://<bronze-bucket>/bronze/transactions/`
directly. Two things changed vs. the original CE plan:

1. CE's `fs.s3a.access.key` Spark-config approach is not the Free
   Edition path -- UC wants a **storage credential + external location**
   instead.
2. Free Edition lists "custom workspace storage locations" as
   unsupported, and storage credentials normally require account-level
   IAM setup that Free Edition restricts.

Whether external S3 read works on Free Edition is **unverified**. If it
doesn't, the fallback is a UC **Volume**: push bronze files up with the
Databricks CLI (`databricks fs cp --recursive ./data/bronze
dbfs:/Volumes/<catalog>/<schema>/bronze/`) and point the notebooks at
the volume path instead. That keeps every other layer unchanged, and the
S3 -> Glue portion of the pipeline still stands on its own as the AWS
half of the architecture.

Resolve this first -- it's the one unknown that changes notebook code.
