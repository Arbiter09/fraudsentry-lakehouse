# governance/ -- lineage and data quality

Lineage comes from two complementary places, and it's worth being clear
about why both exist:

- **Unity Catalog** (built into Databricks Free Edition) captures
  table- and column-level lineage automatically for anything that runs
  inside Databricks -- no setup, no extra services. This is the fastest
  path to a real lineage screenshot, so **start here**.
- **OpenLineage -> DataHub** (below) is what stitches the *non*-Databricks
  half in: S3, the Glue crawler, and the Dagster orchestration layer. UC
  can't see those. Stand this up second, once UC lineage is working.

If you only have time for one, UC lineage covers the "column-level
lineage" interview question with far less operational overhead. DataHub
is what demonstrates cross-tool lineage across a heterogeneous stack.

## Lineage: OpenLineage -> DataHub

DataHub ships its own quickstart, which is the supported way to stand up
the full stack (GMS, frontend, Kafka, Elasticsearch, MySQL) -- not worth
hand-rolling in a compose file here:

```bash
pip install acryl-datahub
datahub docker quickstart   # brings up DataHub at http://localhost:9002
```

Then point both dbt and Dagster at it via OpenLineage:

```bash
cp governance/openlineage.env.example governance/openlineage.env
# edit if your GMS OpenLineage endpoint differs, then:
source governance/openlineage.env

# from dbt/fraudsentry_dbt/, use dbt-ol instead of plain dbt so lineage
# events actually get emitted:
pip install openlineage-dbt
dbt-ol build

# Dagster's dbt integration (dagster-dbt) already emits asset-level
# lineage into the Dagster UI itself; OPENLINEAGE_URL above additionally
# forwards it to DataHub so cross-tool lineage (S3 -> Glue -> Databricks
# -> dbt -> gold) shows up in one graph instead of being split across
# each tool's own UI.
```

## Data quality: Great Expectations

`great_expectations/` (gitignored `uncommitted/` holds local state/creds
per GE convention) validates the bronze layer before it's trusted
downstream -- catching malformed records that got past
`common/validation.py`'s per-record checks but look wrong in aggregate
(e.g. a sudden spike in null merchant categories).

```bash
pip install great_expectations
cd governance/great_expectations
great_expectations init   # first time only
# then define a checkpoint against the S3 bronze prefix or the Glue table
great_expectations checkpoint run bronze_transactions_checkpoint
```

Quality thus has two layers in this project: per-record validation at
ingestion time (`common/validation.py`, enforced by both the local
consumer and the Lambda handler) and aggregate/statistical validation
after landing (Great Expectations on bronze, dbt schema + singular tests
on staging/marts).
