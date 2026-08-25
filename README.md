# fraudsentry-lakehouse

A streaming fraud-detection lakehouse: synthetic transactions flow through Kafka/MSK, land in
an S3-backed medallion architecture, get transformed and scored for anomalies in Databricks,
modeled with dbt, orchestrated with Dagster, and governed with OpenLineage/DataHub + Great
Expectations.

Status: under active development. Full architecture, setup instructions, and screenshots land
as each stage is built — see commit history for progress.

## Stack

- **Streaming**: Kafka (local dev) / AWS MSK (demo)
- **Ingestion**: AWS Lambda
- **Storage**: AWS S3 (bronze/silver/gold), AWS Glue Data Catalog
- **Lakehouse**: Databricks (Delta Lake), Isolation Forest anomaly scoring
- **Transformation & testing**: dbt (dbt-databricks)
- **Orchestration**: Dagster
- **Governance**: OpenLineage, DataHub, Great Expectations
- **IaC**: Terraform

## Layout

```
data_generator/   synthetic transaction generator
ingestion/        Kafka producer/consumer, docker-compose for local dev
lambda/           Lambda handler deployed via Terraform
infra/            Terraform for S3, Glue, Lambda, MSK
databricks/       notebooks (bronze -> silver -> gold -> anomaly scoring)
dbt/              dbt project (silver -> gold models + tests)
dagster/          orchestration assets
governance/       DataHub + Great Expectations config
docs/             architecture diagram, screenshots
```
