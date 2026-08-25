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

    subgraph dbx["Databricks (Community Edition)"]
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
- **Databricks Community Edition** has no Unity Catalog (Glue's Data
  Catalog fills that role here) and uneven Jobs API support, so the
  Dagster -> Databricks edge (`O3 -.-> N1`) may end up being a manual
  trigger rather than a live API call -- see `databricks/README.md` and
  `dagster/fraudsentry_dagster/databricks_asset.py`.
- Quality is layered: per-record validation at ingestion
  (`common/validation.py`, shared by the local consumer and the Lambda
  handler), then aggregate checks on landed bronze data (Great
  Expectations), then dbt schema + singular tests on the
  staging/marts layer.
