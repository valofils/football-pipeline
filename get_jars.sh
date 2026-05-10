#!/usr/bin/env bash
# scripts/get_jars.sh — download Spark JARs for football-pipeline-v8
#
# New in v8: delta-spark and delta-storage JARs added.
#
# Usage: bash scripts/get_jars.sh

set -euo pipefail

JARS_DIR="${JARS_DIR:-spark/jars}"
mkdir -p "$JARS_DIR"

SCALA="2.12"
SPARK_VERSION="3.5.1"
DELTA_VERSION="3.2.0"
HADOOP_VERSION="3.3.4"
AWS_SDK_VERSION="1.12.262"
PG_VERSION="42.7.3"

BASE_MAVEN="https://repo1.maven.org/maven2"

declare -A JARS=(
  # PostgreSQL JDBC driver (v7+)
  ["postgresql-${PG_VERSION}.jar"]="${BASE_MAVEN}/org/postgresql/postgresql/${PG_VERSION}/postgresql-${PG_VERSION}.jar"

  # Hadoop S3A (v7+)
  ["hadoop-aws-${HADOOP_VERSION}.jar"]="${BASE_MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"

  # AWS SDK bundle (v7+)
  ["aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"]="${BASE_MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

  # Delta Lake core (v8 new)
  ["delta-spark_${SCALA}-${DELTA_VERSION}.jar"]="${BASE_MAVEN}/io/delta/delta-spark_${SCALA}/${DELTA_VERSION}/delta-spark_${SCALA}-${DELTA_VERSION}.jar"

  # Delta storage (v8 new) — required by delta-spark at runtime
  ["delta-storage-${DELTA_VERSION}.jar"]="${BASE_MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"
)

for jar in "${!JARS[@]}"; do
  target="${JARS_DIR}/${jar}"
  if [[ -f "$target" ]]; then
    echo "  [skip] $jar already present"
  else
    echo "  [download] $jar"
    curl -fsSL -o "$target" "${JARS[$jar]}"
  fi
done

echo ""
echo "All JARs present in ${JARS_DIR}/"
ls -lh "$JARS_DIR"
