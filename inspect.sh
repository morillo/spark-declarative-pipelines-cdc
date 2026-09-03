#!/usr/bin/env bash
# Inspect the Delta tables the pipeline produced.
set -euo pipefail
cd "$(dirname "$0")"

export SPARK_HOME="$(.venv/bin/python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"

exec .venv/bin/python inspect_tables.py
