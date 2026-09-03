#!/usr/bin/env bash
# Inspect the Delta tables the pipeline produced.
set -euo pipefail
cd "$(dirname "$0")"
export SPARK_HOME="$PWD/spark"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
exec "$SPARK_HOME/bin/spark-submit" inspect_tables.py 2>/dev/null
