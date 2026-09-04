#!/usr/bin/env bash
# One-time project setup. Prerequisite: Apache Spark 4.2.0 installed
# at /usr/local/spark-versions/spark-4.2.0 (see README, "Installing
# Spark"). The shared Spark install is never modified: this project's
# Delta jars live in ./jars and its confs in ./conf, activated per
# run through SPARK_CONF_DIR.
set -euo pipefail
cd "$(dirname "$0")"

SPARK_HOME="${SPARK_HOME:-/usr/local/spark-versions/spark-4.2.0}"
DELTA_VERSION=4.4.0

if [ ! -x "$SPARK_HOME/bin/spark-pipelines" ]; then
  echo "ERROR: Spark 4.2.0 not found at $SPARK_HOME" >&2
  echo "Install it first (see README, 'Installing Spark'), or set" >&2
  echo "SPARK_HOME to your install location." >&2
  exit 1
fi
echo "==> Using Spark at $SPARK_HOME"

echo "==> Creating virtualenv with the Python client dependencies"
python3 -m venv .venv
.venv/bin/pip install --quiet \
  pyyaml pandas pyarrow grpcio grpcio-status googleapis-common-protos \
  zstandard

echo "==> Fetching Delta Lake ${DELTA_VERSION} jars into ./jars"
mkdir -p jars
for u in \
  "https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar" \
  "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"; do
  (cd jars && curl -sfLO "$u")
done

echo "==> Writing project-local Spark confs (./conf, via SPARK_CONF_DIR)"
mkdir -p conf
cat > conf/spark-defaults.conf <<CONF
spark.jars $PWD/jars/delta-spark_2.13-${DELTA_VERSION}.jar,$PWD/jars/delta-storage-${DELTA_VERSION}.jar
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
CONF

echo "==> Writing .env so VS Code / Pylance resolves pyspark from Spark"
PY4J_ZIP="$(basename "$SPARK_HOME"/python/lib/py4j-*.zip)"
cat > .env <<CONF
PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/${PY4J_ZIP}
CONF

echo "==> Done. Next:  ./run.sh   then:  ./inspect.sh   or:  ./shell.sh"
