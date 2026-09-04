#!/usr/bin/env bash
# Inspect the Delta tables the pipeline produced.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
exec "$SPARK_HOME/bin/spark-submit" inspect_tables.py 2>/dev/null
