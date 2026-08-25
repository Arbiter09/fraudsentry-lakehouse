# Architecture

```mermaid
flowchart LR
    subgraph gen["data_generator/"]
        G[Synthetic transaction generator]
    end

    subgraph stream["Kafka / MSK"]
        K[("transactions topic")]
    end

    subgraph ingest["Lambda"]
        L[lambda/handler.py]
    end

    subgraph lake["S3"]
        B[(bronze/)]
        S[(silver/)]
        Gd[(gold/)]
    end

    subgraph catalog["Glue"]
        C[Data Catalog + Crawler]
    end

    subgraph dbx["Databricks (Free Edition)"]
        N1[01_bronze_to_silver]
        N2[02_silver_to_gold]
        N3[03_anomaly_detection]
    end

    subgraph transform["dbt"]
        T1[stg_transactions]
        T2[fct_daily_account_summary]
    end

    subgraph orch["Dagster"]
        O1[bronze_catalog_refreshed]
        O2[fraudsentry_dbt_assets]
        O3[gold_scored_transactions]
    end

    subgraph gov["Governance"]
        GE[Great Expectations]
        OL[OpenLineage]
        DH[DataHub]
    end

    G --> K --> L --> B
    B --> C
    C --> N1
    N1 --> S --> N2 --> Gd --> N3
    S --> T1 --> T2
    B -.validated by.-> GE
    O1 --> O2 --> O3
    O1 -.-> C
    O3 -.-> N1
    T1 -.emits.-> OL
    O2 -.emits.-> OL
    OL --> DH
```

## Notes

- **MSK is not always-on.** It's provisioned only for short demo
  sessions (`infra/msk.tf`, gated by `var.enable_msk`); day-to-day
  development runs against local Kafka (`ingestion/docker-compose.yml`).
  The producer/consumer code is identical either way -- only the
  bootstrap-servers endpoint changes.
- **Databricks Free Edition** (Community Edition was retired
  2026-01-01) is serverless-only and quota-limited, but does include
  Unity Catalog and Jobs -- so the Dagster -> Databricks edge
  (`O3 -.-> N1`) is expected to be a real API call, not a manual step.
  The open question is the `C --> N1` edge: whether Free Edition can
  read the S3 bronze layer via a UC external location, or whether data
  needs staging into a UC Volume first. See `databricks/README.md`.
- **Two catalogs, on purpose.** Glue catalogs the S3 side (and is what
  the Dagster crawler asset refreshes); Unity Catalog governs the
  Databricks side and supplies column-level lineage. They meet at the
  bronze layer.
- Quality is layered: per-record validation at ingestion
  (`common/validation.py`, shared by the local consumer and the Lambda
  handler), then aggregate checks on landed bronze data (Great
  Expectations), then dbt schema + singular tests on the
  staging/marts layer.
