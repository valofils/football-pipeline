#!/usr/bin/env bash
# Download JARs required by Spark for PostgreSQL JDBC + S3A access.
# Run once before `docker compose up`.
set -euo pipefail

JARS_DIR="$(dirname "$0")/../spark/jars"
mkdir -p "$JARS_DIR"

POSTGRES_JAR="https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
HADOOP_AWS_JAR="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
AWS_SDK_JAR="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

download() {
  local url="$1"
  local dest="$JARS_DIR/$(basename "$url" | sed 's/-[0-9].*//' ).jar"
  if [ -f "$dest" ]; then
    echo "✓  $(basename "$dest") already present"
  else
    echo "↓  Downloading $(basename "$url") …"
    curl -sSL "$url" -o "$dest"
    echo "✓  Saved to $dest"
  fi
}

download "$POSTGRES_JAR"
download "$HADOOP_AWS_JAR"
download "$AWS_SDK_JAR"

echo ""
echo "All JARs ready in $JARS_DIR"
ls -lh "$JARS_DIR"
