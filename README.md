# football-pipeline-v3

Premier League match data pipeline orchestrated with Apache Airflow.
Builds on v1 (Python/Parquet lake) and v2 (PostgreSQL + pytest), adding
scheduled, monitored, retryable DAG execution.

## What it does

A weekly Airflow DAG ingests Premier League CSV data, enforces a PyArrow schema,
writes a partitioned Parquet lake, upserts rows into PostgreSQL, and rebuilds a
standings view — all with automatic retries and full observability in the Airflow UI.

```
ingest → validate → load_postgres → build_standings
```

## Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9.1 (LocalExecutor) |
| Language | Python 3.12 |
| Data | pandas 2.x · PyArrow 15 · Parquet/Snappy |
| Storage | PostgreSQL 16 |
| Testing | pytest · pytest-cov |
| Infrastructure | Docker Compose |

## Quickstart

```bash
mkdir -p dags plugins logs data/raw data/parquet
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up -d
open http://localhost:8080   # admin / admin
```

Drop CSVs into `data/raw/`, then trigger the DAG from the UI.

## CSV schema

| Column | Type | Example |
|---|---|---|
| `match_id` | integer | `1` |
| `season` | string | `2023-24` |
| `home_team` | string | `Arsenal` |
| `away_team` | string | `Chelsea` |
| `home_goals` | integer | `2` |
| `away_goals` | integer | `1` |
| `match_date` | date (YYYY-MM-DD) | `2023-08-12` |

## DAG

**ID:** `football_pipeline` · **Schedule:** `@weekly` · **Catchup:** disabled · **Retries:** 2 per task

| Task | What it does |
|---|---|
| `ingest` | Reads CSVs, enforces PyArrow schema, writes partitioned Parquet |
| `validate` | Asserts non-zero row count and no nulls in key columns |
| `load_postgres` | Creates `matches` table if absent; upserts rows via `PostgresHook` |
| `build_standings` | Rebuilds `standings` view with points and goal difference |

## Running tests

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v --cov=dags --cov-report=term-missing
```

## Skills introduced

- Apache Airflow TaskFlow API (`@dag`, `@task`)
- DAG parameters: `schedule`, `start_date`, `catchup`, `default_args`
- XCom — passing data between tasks via return values
- `PostgresHook` — managed database connections in Airflow
- `DagBag` — loading and inspecting DAGs in pytest
- Accessing task inner functions via `.function` for unit testing
- `docker-compose` multi-service orchestration with health checks
- YAML anchors (`&`, `<<:`) for DRY service configuration

## Learning path

```
v1  Python · pandas · PyArrow · Parquet · argparse CLI
v2  PostgreSQL · psycopg2 · pytest · window functions · upsert
v3  Airflow · DAGs · scheduling · XCom · PostgresHook · Docker  ← you are here
v4  Apache Spark  (next)
```

## License

MIT
