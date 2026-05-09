# Football Analytics Pipeline

A data engineering pipeline that ingests raw football match CSV data, enriches it with derived statistics, stores it as a partitioned Parquet data lake, and exposes a CLI for querying team and match stats.

Built to demonstrate core Python data engineering skills: `pathlib`, list comprehensions, `pandas` transforms, `pyarrow` schema enforcement, partitioned Parquet writes, and `argparse` CLI design.

---

## Project structure

```
football-pipeline/
├── pipeline/
│   ├── ingest.py       # CSV → validated PyArrow table
│   ├── transform.py    # pandas enrichment (8 derived columns)
│   ├── store.py        # partitioned Parquet write via pyarrow
│   └── query.py        # stats from Parquet with predicate pushdown
├── data/
│   ├── raw/
│   │   └── matches.csv # sample Premier League match data
│   └── lake/           # generated — partitioned by season
├── cli.py              # argparse entrypoint
├── requirements.txt
└── Makefile
```

---

## Quickstart

```bash
git clone https://github.com/your-username/football-pipeline.git
cd football-pipeline

pip install -r requirements.txt

# 1. Run the full pipeline (ingest → transform → store)
python cli.py ingest --input data/raw/matches.csv

# 2. Query the results
python cli.py query --stat standings
python cli.py query --stat team --team Arsenal
python cli.py query --stat top --n 5
python cli.py query --stat high --min-goals 4
python cli.py query --stat referee
```

Or use Make shortcuts:

```bash
make install
make ingest
make standings
make top
make referee
```

---

## Sample output

```
Pos  Team                        P   W   D   L   GF   GA   GD  Pts
-----------------------------------------------------------------
1    Man City                    5   5   0   0   16    4   12   15
2    Arsenal                     7   4   2   1   14    7    7   14
3    Liverpool                   5   4   1   0   10    4    6   13
...

Top 5 highest-scoring matches:
  Arsenal 4-2 Leicester                    6 goals
  Man City 5-1 Wolves                      6 goals
  Chelsea 3-2 Wolves                       5 goals
```

---

## Skills demonstrated

| Concept | Where |
|---|---|
| `pathlib` for safe path handling | `ingest.py`, `store.py` |
| List comprehensions | `ingest.py` (transpose), `transform.py` (derived cols), `query.py` |
| `pyarrow` schema enforcement | `ingest.py` — types validated at load time |
| pandas method chaining | `transform.py` — `groupby → agg → sort_values → reset_index` |
| Partitioned Parquet writes | `store.py` — `pq.write_to_dataset(partition_cols=["season"])` |
| Predicate + projection pushdown | `query.py` — `pq.read_table(columns=..., filters=...)` |
| `argparse` subcommands | `cli.py` — `ingest` and `query` with typed arguments |

---

## Extending the project

- Add a `tests/` folder with `pytest` unit tests for each module
- Swap the sample CSV for real data from the [football-data.co.uk](https://www.football-data.co.uk/) dataset
- Add a `visualise.py` module using `matplotlib` for standings charts
- Replace the local lake with S3 using `pyarrow`'s `S3FileSystem`

---

## Requirements

- Python 3.11+
- pandas >= 2.2
- pyarrow >= 15.0
