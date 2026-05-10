-- scripts/init_mlflow_db.sql
-- Runs once inside the airflow-db Postgres container on first start.
-- Creates the separate 'mlflow' database and user required by the
-- MLflow tracking server backend store.

CREATE USER mlflow WITH PASSWORD 'mlflow';
CREATE DATABASE mlflow OWNER mlflow;
GRANT ALL PRIVILEGES ON DATABASE mlflow TO mlflow;
