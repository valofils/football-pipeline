# football-pipeline-v6 — Kafka + Airflow + Spark + dbt

Extends v5 by adding **Apache Kafka** before the Airflow pipeline.
Instead of dropping CSVs into `data/raw/`, match events are produced to a
Kafka topic and consumed into PostgreSQL in near-real-time.

```
CSV files  →  Producer  →  Kafka topic `match-events`
                                   ↓
                           Airflow DAG (weekly)
                                   ↓
               ┌───────────────────────────────────────┐
               │  kafka_ingest  →  validate  →          │
               │  load_postgres  →  dbt_run             │
               └───────────────────────────────────────┘
                                   ↓
                         PostgreSQL  →  dbt models
                         (matches_staging, matches, standings)
```

---

## What's new in v6

| Component | Change |
|---|---|
| `kafka/producer/produce_matches.py` | Reads CSVs, publishes one JSON message per match to `match-events` |
| `kafka/consumer/consume_matches.py` | Consumes topic, bulk-upserts into `matches_staging` |
| `dags/football_pipeline.py` | `ingest` task replaced by `kafka_ingest` (calls consumer) |
| `docker-compose.yaml` | Adds `zookeeper`, `kafka`, `kafka-ui` services |
| `tests/test_dag.py` | 35 tests across 7 classes (adds `TestProducer`, `TestConsumer`, `TestConsumerUpsert`) |

---

## Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Messaging | Apache Kafka 3.7 · confluent-kafka 2.4.0 |
| DataFrames | PySpark 3.5.1 |
| Serialisation | JSON (messages) · Parquet/Snappy (lake) |
| Database | PostgreSQL 16 · psycopg2 · JDBC |
| Modelling | dbt-core 1.8.3 · dbt-postgres |
| Testing | pytest · pytest-cov · dbt test |
| Orchestration | Apache Airflow 2.9.1 · LocalExecutor · PostgresHook |
| Compute | Apache Spark 3.5.1 standalone cluster |
| Infrastructure | Docker Compose |

---

## Quickstart

### 1. Start the stack

```bash
docker compose up -d
```

Services and ports:

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8081 (admin / admin) |
| Kafka UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8082 |
| Football DB | localhost:5433 |

### 2. Download the PostgreSQL JDBC JAR (one-time)

```bash
mkdir -p spark/jars
curl -L -o spark/jars/postgresql.jar \
  https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

### 3. Produce match events

Run the producer against your CSV files (from outside Docker, using the
EXTERNAL Kafka listener on port 9093):

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9093 \
  python kafka/producer/produce_matches.py --data-dir data/raw/
```

Or for a single season:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9093 \
  python kafka/producer/produce_matches.py --data-dir data/raw/ --season 2023-24
```

### 4. Trigger the DAG

In the Airflow UI, enable and trigger `football_pipeline` manually, or wait
for the `@weekly` schedule.

The `kafka_ingest` task will consume all pending messages from `match-events`
and upsert them into `matches_staging`.

### 5. Run tests

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=. --cov-report=term-missing
```

DB-dependent tests (`TestConsumerUpsert`) are skipped automatically if
`football-db` is not reachable on `localhost:5433`.

---

## Key Kafka concepts introduced in v6

### Topic: `match-events`

- **Partitions**: 3 (parallel consumption; messages keyed by `season:match_id`
  so all matches for a season land in the same partition)
- **Retention**: 7 days (`retention.ms`)
- **Cleanup policy**: `delete` (segments expire; no compaction)

### Producer config

```python
"acks": "all"          # Wait for leader + all ISRs to ack
"retries": 5           # Retry transient errors
"linger.ms": 10        # Batch small messages for throughput
```

### Consumer config

```python
"auto.offset.reset": "earliest"   # Start from beginning if no committed offset
"enable.auto.commit": False        # Manual commit — only after successful DB write
```

### Offset management

The consumer:
1. Snapshots the **high-water mark** (end offset) for each partition at startup
2. Reads until all partitions reach their high-water mark (catches up to
   messages that existed when the job started)
3. **Commits offsets manually** after each successful DB upsert batch
4. On the next DAG run, consumption resumes from the last committed offset
   (`earliest` fallback only applies to a brand-new consumer group)

### Airflow integration

`kafka_ingest` is a plain `@task`-decorated Python callable — no Kafka-specific
Airflow operator required.  The consumer runs as a **finite batch job**
(not a long-running daemon): it catches up to the high-water mark and exits.

---

## Project structure

```
football-pipeline-v6/
├── kafka/
│   ├── producer/
│   │   └── produce_matches.py
│   └── consumer/
│       └── consume_matches.py
├── dags/
│   ├── football_pipeline.py
│   └── spark_utils.py
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   └── stg_matches.sql
│   │   └── marts/
│   │       ├── standings.sql
│   │       └── schema.yml
│   ├── tests/
│   │   └── assert_positive_goals.sql
│   └── profiles.yml
├── tests/
│   ├── conftest.py
│   └── test_dag.py
├── spark/
│   └── jars/          ← place postgresql.jar here
├── docker-compose.yaml
└── requirements.txt
```
