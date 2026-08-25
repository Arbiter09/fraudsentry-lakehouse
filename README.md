# fraudsentry-lakehouse

A streaming fraud-detection lakehouse, built end-to-end as a portfolio project:
synthetic transactions flow through Kafka/MSK, land in an S3-backed
medallion architecture, get transformed and anomaly-scored in Databricks,
modeled and tested with dbt, orchestrated with Dagster, and governed with
OpenLineage/DataHub + Great Expectations.

See [`docs/architecture.md`](docs/architecture.md) for the full diagram.

## Why it's built this way

Two constraints shaped every design decision here, and they're both
**deliberate, not accidental**:

- **AWS free tier only.** Everything runs within free-tier limits except
  MSK, which has no free tier at all (it bills per broker-hour). So MSK
  is gated off by default (`infra/variables.tf`'s `enable_msk = false`)
  and only stood up for short demo sessions, then torn down. Local
  development runs against a Dockerized Kafka broker instead — the
  producer/consumer code is identical either way, only the bootstrap
  server address changes.
- **Databricks Community Edition.** No Unity Catalog (the Glue Data
  Catalog fills that role instead) and inconsistent Jobs API support for
  external orchestration. The Dagster → Databricks step
  (`dagster/fraudsentry_dagster/databricks_asset.py`) makes a real API
  call and documents exactly what to do if your workspace rejects it
  (run the notebooks manually).

If you're reading this as a hiring manager or interviewer: that's the
point of building it this way — it's a real story about working within
platform/cost constraints, not just gluing together a checklist of tool
names.

## Stack

| Layer | Tool |
|---|---|
| Streaming | Kafka (local) / AWS MSK (demo) |
| Ingestion | AWS Lambda |
| Storage | AWS S3 (bronze/silver/gold), AWS Glue Data Catalog |
| Lakehouse + ML | Databricks (Delta Lake), Isolation Forest |
| Transformation + tests | dbt (dbt-databricks) |
| Orchestration | Dagster |
| Governance | OpenLineage, DataHub, Great Expectations |
| IaC | Terraform |

## Repo layout

```
data_generator/   synthetic transaction generator (normal + fraud patterns)
common/           validation rules shared by the local consumer and the Lambda handler
ingestion/        Kafka producer + local bronze consumer, docker-compose for Kafka
lambda/           Lambda handler deployed via Terraform, triggered by MSK
infra/            Terraform: S3, Glue, Lambda, MSK (gated behind enable_msk)
databricks/       Community Edition notebooks: bronze -> silver -> gold -> anomaly scoring
dbt/              dbt project: staging + marts models, schema + custom tests
dagster/          orchestration: Glue crawler asset, dbt assets, Databricks job asset
governance/       OpenLineage/DataHub setup, Great Expectations suite
docs/             architecture diagram
```

## Quickstart (local pipeline, no AWS/Databricks needed)

This gets synthetic transactions flowing end-to-end into a local bronze
layer — the fastest way to see the pipeline actually run.

```bash
# 1. Install Python deps
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Start local Kafka
cd ingestion && docker compose up -d && cd ..

# 3. In one terminal: consume + validate + write bronze
python3 ingestion/consumer.py --out-dir data/bronze

# 4. In another terminal: produce synthetic transactions
python3 ingestion/producer.py --rate 10 --fraud-rate 0.05 --count 500

# 5. Check the output
ls data/bronze/
cat data/bronze/dt=*/bronze.ndjson | head
cat data/bronze/_dead_letter.ndjson 2>/dev/null   # should be empty/absent
```

You can also generate a one-shot batch file without Kafka at all:

```bash
python3 data_generator/generate_transactions.py --count 1000 --fraud-rate 0.02 --out sample.ndjson
```

## Running the rest of the pipeline

Each of these needs real accounts/credentials, so they're documented
per-layer rather than folded into one script:

- **AWS (S3/Glue/Lambda, optionally MSK):** [`infra/README.md`](infra/README.md) —
  Terraform package/apply/destroy sequence, with explicit cost notes.
- **Databricks notebooks:** [`databricks/README.md`](databricks/README.md) —
  import order, Community Edition credential setup, orchestration caveats.
- **dbt:** from `dbt/fraudsentry_dbt/`, standard `dbt deps && dbt build`
  once your `~/.dbt/profiles.yml` points at the Databricks silver/gold
  schema.
- **Dagster:** from `dagster/`, `pip install -e .` then
  `dagster dev -m fraudsentry_dagster.definitions` to open the asset
  graph UI at `localhost:3000`.
- **Governance (OpenLineage/DataHub, Great Expectations):**
  [`governance/README.md`](governance/README.md).

## Data quality, in two layers

1. **Per-record, at ingestion** — `common/validation.py`, enforced
   identically by the local consumer and the Lambda handler (required
   fields, positive amount, parseable timestamp). Invalid records go to
   a dead-letter path (local file / SQS) instead of failing the batch.
2. **Aggregate, after landing** — a Great Expectations suite on bronze
   (`governance/great_expectations/`) plus dbt schema and custom
   singular tests on staging/marts (`dbt/fraudsentry_dbt/tests/`) catch
   issues that only show up in aggregate, like a sudden spike in nulls
   or an empty crawl.

## Status

Actively under development — see commit history for what's landed vs.
in progress. Screenshots of the Dagster asset graph, DataHub lineage
view, and Databricks notebook results get added to `docs/` as each
piece is run against real infrastructure.
