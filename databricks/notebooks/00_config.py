# Databricks notebook source
# MAGIC %md
# MAGIC # Shared config
# MAGIC Single source of truth for catalog/schema/volume names and table
# MAGIC identifiers. The other notebooks pull these in with
# MAGIC `%run ./00_config`, so switching catalogs or moving from the UC
# MAGIC Volume to an S3 external location is a one-file change.
# MAGIC
# MAGIC **Always use three-part names** (`catalog.schema.table`) under Unity
# MAGIC Catalog. A two-part name resolves against whatever catalog happens
# MAGIC to be current, which silently writes tables into the wrong place.

# COMMAND ----------

CATALOG = "fraudsentry"
SCHEMA = "fraudsentry"

# Where bronze data is read from.
#
# Option A -- UC Volume (works on Free Edition today; stage files with the
# Catalog Explorer upload button or `databricks fs cp`):
BRONZE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/bronze/"
#
# Option B -- S3 external location, once Terraform has created the bucket
# and you've registered a UC storage credential + external location over
# it. Swap to this and nothing else in the pipeline changes:
# BRONZE_PATH = "s3://<bronze-bucket>/bronze/transactions/"

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_transactions"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_account_features"
SCORED_TABLE = f"{CATALOG}.{SCHEMA}.gold_scored_transactions"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"catalog={CATALOG} schema={SCHEMA}")
print(f"bronze={BRONZE_PATH}")
print(f"silver={SILVER_TABLE}")
print(f"gold={GOLD_TABLE}")
print(f"scored={SCORED_TABLE}")
