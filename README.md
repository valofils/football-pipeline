# football-pipeline-v8 — Delta Lake on S3

> Builds on v7 (Kafka + Spark + Airflow + dbt + AWS S3/RDS + Terraform).
> One new concept layer: **Delta Lake** replaces raw Parquet on S3.

## What's new in v8

| Concern | v7 | v8 |
|---|---|---|
| Storage format | Raw Parquet on S3 | **Delta Lake on S3** |
| Upsert at storage | None (only at Postgres layer) | `MERGE INTO` via `DeltaTable.merge()` |
| Schema enforcement | Spark StructType on read | Delta protocol + StructType |
| Schema evolution | Manual | `mergeSchema` option |
| Time travel | None | `versionAsOf` / `timestampAsOf` |
| ACID transactions | None | Delta transaction log |
| New JAR deps | — | `delta-spark_2.12-3.2.0.jar`, `delta-storage-3.2.0.jar` |
| New Python dep | — | `delta-spark==3.2.0` |

## Architecture

```
CSV → Kafka producer
         │
         ▼
    Kafka topic (football-matches)
         │
         ▼  kafka_ingest task
    Delta Lake (s3a://bucket/delta/matches)   ◄── NEW
         │              │
         │              └─ _delta_log/ (ACID, time travel)
         ▼  validate task
    validate nulls (reads Delta)
         │
         ▼  load_postgres task
    RDS PostgreSQL (matches table)
         │
         ▼  dbt_run task
    dbt: stg_matches → standings (incremental)
```

## Key Delta mental models

### Delta transaction log
Every write appends a JSON entry to `_delta_log/`. This log is the source of
truth for the table — Delta reconstructs the current snapshot by replaying it.
Spark never modifies existing Parquet files; it only adds new ones.

### MERGE INTO (storage-layer upsert)
```python
DeltaTable.forPath(spark, path)
    .alias("existing")
    .merge(new_df.alias("incoming"), "existing.match_id = incoming.match_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
```
This is the Delta equivalent of PostgreSQL's `ON CONFLICT DO UPDATE`.
It runs entirely at the storage layer — no Postgres required.

### Time travel
```python
# Read the table as it was at version 0 (initial load)
spark.read.format("delta").option("versionAsOf", 0).load(path)

# Or by timestamp
spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)
```
Every `write_delta` / `merge_delta` call increments the version counter.

### Why Delta over raw Parquet?
| Problem with raw Parquet | Delta solution |
|---|---|
| Partial write = corrupt table | Atomic commits via log |
| No upsert primitive | `MERGE INTO` |
| Schema drift silently breaks reads | Schema enforcement + evolution |
| No audit trail | Full history in `_delta_log/` |
| Compaction is manual | `OPTIMIZE` + `ZORDER` commands |

## Quick start

```bash
# 1. Download JARs (adds Delta JARs to spark/jars/)
bash scripts/get_jars.sh

# 2. Provision AWS infra (unchanged from v7)
cd terraform && terraform init && terraform apply

# 3. Export env vars (add to .env)
export S3_BUCKET_NAME=<terraform output s3_bucket_name>
export FOOTBALL_DB_HOST=<terraform output rds_host>
export FOOTBALL_DB_USER=football
export FOOTBALL_DB_PASSWORD=<your password>
export AWS_ACCESS_KEY_ID=<your key>
export AWS_SECRET_ACCESS_KEY=<your secret>

# 4. Start services
docker compose up -d

# 5. Publish a CSV to Kafka
python kafka/producer/produce_matches.py --file data/raw/matches.csv

# 6. Trigger DAG
airflow dags trigger football_pipeline

# 7. Run tests
pytest tests/ -v --cov=dags --cov-report=term-missing
```

## Inspecting the Delta table

```python
from delta.tables import DeltaTable
from dags.spark_utils import get_spark, delta_path

spark = get_spark()

# Current snapshot
df = spark.read.format("delta").load(delta_path("matches"))
df.show()

# History
DeltaTable.forPath(spark, delta_path("matches")).history().show(truncate=False)

# Time travel
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_path("matches"))
df_v0.show()
```

## Full stack (v8)

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Messaging | Apache Kafka 3.7 · confluent-kafka 2.4.0 |
| DataFrames | PySpark 3.5.1 |
| **Storage format** | **Delta Lake 3.2.0** (replaces raw Parquet) |
| Serialisation | JSON (messages) · PyArrow 15 · Parquet/Snappy (Delta files) |
| Database | PostgreSQL 16 · psycopg2 · JDBC · AWS RDS |
| Object storage | AWS S3 · hadoop-aws · s3a:// |
| Modelling | dbt-core 1.8.3 · dbt-postgres |
| Testing | pytest · pytest-cov · dbt test · moto |
| Orchestration | Apache Airflow 2.9.1 · LocalExecutor · PostgresHook |
| Compute | Apache Spark 3.5.1 standalone cluster |
| Infrastructure | Docker Compose · Terraform 1.7+ |
