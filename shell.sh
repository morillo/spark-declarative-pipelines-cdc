#!/usr/bin/env bash
# Interactive Spark shell with Delta configured and `spark` ready.
# (The pip distribution's `pyspark` launcher script is broken in
# 4.2.0 — its connect-mode probe fails — so this uses python -i.)
set -euo pipefail
cd "$(dirname "$0")"
export SPARK_HOME="$(.venv/bin/python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
exec .venv/bin/python -i -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('shell').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
print()
print('SparkSession ready as: spark')
print(\"try: spark.read.format('delta').load('pipeline/spark-warehouse/customers_silver').show()\")"
