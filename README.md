# football-pipeline-v4

Builds on [v3](https://github.com/you/football-pipeline-v3) (Airflow orchestration),
replacing **pandas** with **Apache Spark** for distributed DataFrame processing.

## What changed from v3

| Task | v3 | v4 |
|---|---|---|
| `ingest` | `pandas.read_csv` + `pyarrow.write_to_dataset` | `spark.read.csv` + `df.write.parquet` |
| `validate` | `df.isnull()`, `len(df)` | `df.filter(col.isNull()).count()` |
| `load_postgres` | `psycopg2` / `PostgresHook.run` | `df.write.jdbc` + `PostgresHook.run` (upsert) |
| `build_standings` | `PostgresHook.run` SQL | `spark.read.jdbc` → `spark.sql` → `df.write.jdbc` |

The four-task chain, `@weekly` schedule, retries, and XCom contracts are **unchanged** from v3. Only the compute layer is different.

## What it does

A weekly Airflow DAG ingests Premier League CSV data, enforces a PySpark schema, writes a
partitioned Parquet lake, upserts rows into PostgreSQL via JDBC, and rebuilds a standings
table — all orchestrated with automatic retries and full Airflow UI observability.

```
ingest >> validate >> load_postgres >> build_standings
```

## Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9.1 (LocalExecutor) |
| Compute | Apache Spark 3.5.1 (standalone cluster) |
| Language | Python 3.12 · PySpark 3.5.1 |
| Data | PyArrow 15 · Parquet/Snappy |
| Storage | PostgreSQL 16 (via JDBC) |
| Testing | pytest · pytest-cov |
| Infrastructure | Docker Compose |

## Project structure

```
football-pipeline-v4/
├── dags/
│   ├── football_pipeline_dag.py   # TaskFlow DAG — four Spark tasks
│   └── spark_utils.py             # SparkSession factory, MATCH_SCHEMA, JDBC helpers
├── tests/
│   ├── conftest.py                # session-scoped SparkSession fixture
│   └── test_dag.py                # 5 test classes, 25 tests
├── data/
│   ├── raw/                       # drop CSVs here
│   └── parquet/                   # written by the ingest task (git-ignored)
├── spark/
│   └── jars/                      # place postgresql-42.7.3.jar here (see Quickstart)
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── README.md
```

## Quickstart

**Prerequisites:** Docker Desktop · Python 3.12

```bash
git clone https://github.com/you/football-pipeline-v4
cd football-pipeline-v4

# 1 — download the PostgreSQL JDBC driver (required for Spark <-> Postgres)
mkdir -p spark/jars
curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
     -o spark/jars/postgresql-42.7.3.jar

# 2 — create dirs and set Airflow UID
mkdir -p dags plugins logs data/raw data/parquet
echo "AIRFLOW_UID=$(id -u)" > .env

# 3 — initialise Airflow (DB migrate + admin user + football_db connection)
docker compose up airflow-init

# 4 — start all services
docker compose up -d
#   Airflow UI  -> http://localhost:8080  (admin / admin)
#   Spark UI    -> http://localhost:8081
#   Football DB -> localhost:5433

# 5 — drop CSVs and trigger
docker compose exec airflow-scheduler airflow dags trigger football_pipeline
```

## Running tests

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

pytest tests/ -v --cov=dags --cov-report=term-missing
```

Tests run fully offline with `local[2]` Spark — no Docker required.

### Test coverage

| Class | What is tested |
|---|---|
| `TestDagStructure` | DAG loads, task IDs, dependency chain, schedule, retries, tags |
| `TestIngestTask` | CSV read, schema enforcement, Parquet roundtrip, `FileNotFoundError`, partition dirs |
| `TestValidateTask` | clean data passes, empty DataFrame raises, null column raises, negative goals raises |
| `TestLoadPostgresTask` | JDBC url format, properties keys, staging table target, hook call count |
| `TestBuildStandingsTask` | SQL runs, columns present, point totals, goal diff, all teams represented, `UNION ALL` |

## Key Spark concepts introduced

| Concept | Where |
|---|---|
| `SparkSession` | `spark_utils.get_spark()` — single entry point; reused across tasks |
| `spark.read.csv` with explicit schema | `ingest` — replaces `pandas.read_csv` + PyArrow schema |
| `df.write.partitionBy` | `ingest` — replicates v1's partitioned Parquet layout |
| `df.filter(col.isNull()).count()` | `validate` — Spark null check vs pandas `isna()` |
| `df.write.jdbc` | `load_postgres` — parallel write to Postgres; staging + upsert pattern |
| `spark.read.jdbc` | `build_standings` — reads matches table into a DataFrame |
| `createOrReplaceTempView` + `spark.sql` | `build_standings` — runs v2 UNION ALL standings SQL unchanged |
| Lazy evaluation | all tasks — transformations build a plan; `.count()` / `.write` trigger execution |
| `scope="session"` SparkSession fixture | `conftest.py` — one JVM startup per test run |

## XCom data contract

```python
# ingest
{"files_ingested": 1, "rows_written": 8, "parquet_dir": "/opt/airflow/data/parquet"}

# validate  (extends ingest)
{..., "rows_validated": 8, "validation_passed": True}

# load_postgres  (extends validate)
{..., "rows_loaded": 8}

# build_standings  (extends load_postgres)
{..., "standings_rows": 16, "seasons_processed": ["2024"]}
```

## Querying standings

```bash
docker compose exec football-db psql -U football -d football
```

```sql
SELECT team, played, wins, draws, losses, points, goal_diff
FROM   standings
WHERE  season = '2024'
ORDER  BY points DESC, goal_diff DESC;
```

## Learning path

```
v1  Python · pathlib · pandas · PyArrow · Parquet · argparse CLI
v2  PostgreSQL · psycopg2 · pytest · window functions · upsert
v3  Airflow · TaskFlow API · XCom · PostgresHook · Docker Compose
v4  Apache Spark · SparkSession · DataFrame API · JDBC · spark.sql()   <- you are here
v5  dbt · data modelling · incremental models · tests · sources
```

## License

MIT
