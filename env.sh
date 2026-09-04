# Shared environment for the project scripts (sourced, not run).
SPARK_HOME="${SPARK_HOME:-/usr/local/spark-versions/spark-4.2.0}"
export SPARK_HOME
export SPARK_CONF_DIR="$PWD/conf"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
