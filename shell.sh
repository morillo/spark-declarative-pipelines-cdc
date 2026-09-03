#!/usr/bin/env bash
# Interactive PySpark shell from the pinned distribution, with Delta
# configured (spark/conf) and the `spark` session ready.
set -euo pipefail
cd "$(dirname "$0")"
export SPARK_HOME="$PWD/spark"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
exec "$SPARK_HOME/bin/pyspark"
