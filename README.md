# football-pipeline-v9 — Great Expectations data quality

Builds directly on v8 (Delta Lake on S3). Replaces the hand-rolled null
checks in `validate` with a full Great Expectations expectations suite,
a checkpoint that persists results to S3, and auto-generated Data Docs.

---

## What changed from v8

| Component | v8 | v9 |
|---|---|---|
| `validate` task | `df.filter(col.isNull()).count()` | `validate_dataframe()` → GE checkpoint |
| Expectation store | — | S3: `s3://<bucket>/great_expectations/expectations/` |
| Validation results | — | S3: `s3://<bucket>/great_expectations/validations/` |
| Data Docs | — | S3: `s3://<bucket>/great_expectations/data_docs/index.html` |
| Failure mode | Airflow task raises on null | `DataQualityError` with structured summary |
| New files | — | `gx/`, `dags/gx_utils.py` |

No new Docker services. No new infrastructure — GE stores use the S3
bucket already provisioned by Terraform in v7.

---

## New files

```
gx/
  great_expectations.yml          # data context — S3 stores + spark datasource
  expectations/
    matches_suite.json            # 15 expectations (nulls, ranges, uniqueness, regex)
  checkpoints/
    matches_checkpoint.yml        # runs suite, stores results, updates Data Docs
dags/
  gx_utils.py                     # build_context, validate_dataframe, DataQualityError
tests/
  conftest.py                     # ge_context, ge_suite, ge_validator, mock_validate fixtures
  test_dag.py                     # 50 tests, 10 classes (TestDataQuality is new)
```

---

## Expectations in `matches_suite.json`

| # | Expectation | Columns / scope |
|---|---|---|
| 1 | `expect_table_columns_to_match_ordered_list` | all 7 columns in order |
| 2–6 | `expect_column_values_to_not_be_null` | match_id, season, home_team, away_team, home_goals, away_goals |
| 7 | `expect_column_values_to_be_unique` | match_id |
| 8–9 | `expect_column_values_to_be_of_type` | home_goals → int, away_goals → int |
| 10–11 | `expect_column_values_to_be_between` | home_goals [0,20], away_goals [0,20] |
| 12 | `expect_column_values_to_match_regex` | season → `^\d{4}-\d{2}$` |
| 13 | `expect_table_row_count_to_be_between` | [1, 500] |
| 14 | `expect_column_pair_values_to_not_be_equal` | home_team ≠ away_team |
| 15 | `expect_column_values_to_not_be_null` | away_goals |

---

## DAG flow

```
kafka_ingest  →  validate  →  load_postgres  →  dbt_run
                    │
                    ▼  (on failure)
              DataQualityError
              (task marked FAILED, downstream skipped)
                    │
                    ▼  (always, via GE action list)
              S3: validations JSON + Data Docs HTML
```

---

## Key mental models

### Expectation suite
JSON document declaring what "good data" looks like. Decoupled from code
— edit the JSON, no Python changes needed.

### Checkpoint
Wires a batch (a live DataFrame) to a suite, then fires a list of
**actions** after validation:
- `StoreValidationResultAction` → writes the JSON result to S3
- `UpdateDataDocsAction` → rebuilds the HTML Data Docs site on S3

### Data Docs
Auto-generated static HTML site. Browse to:
```
s3://<bucket>/great_expectations/data_docs/index.html
```
Shows pass/fail per run, per expectation, with trend charts. Serve
locally with `aws s3 cp --recursive` or mount the S3 prefix behind
CloudFront.

### DataQualityError
Raised by `validate_dataframe()` when `checkpoint_result.success` is
False. Airflow marks the task as FAILED; `load_postgres` and `dbt_run`
are skipped. The structured `summary` dict on the exception carries
the list of violated expectations — visible in Airflow task logs.

### Execution engine
GE 0.18 uses a **SparkDFExecutionEngine**, so validation runs inside
the same Spark session that reads the Delta table. No data leaves Spark.

---

## Running tests

```bash
pytest tests/ -v --cov=dags --cov-report=term-missing
```

Expected: **50 passed**.

---

## Viewing Data Docs locally

```bash
aws s3 cp s3://<bucket>/great_expectations/data_docs/ ./data_docs --recursive
open data_docs/index.html
```
