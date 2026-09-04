#!/usr/bin/env bash
# Interactive PySpark shell from the shared Spark install, with this
# project's Delta confs (SPARK_CONF_DIR) and `spark` ready.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
exec "$SPARK_HOME/bin/pyspark"
