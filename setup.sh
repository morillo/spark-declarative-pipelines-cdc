#!/usr/bin/env bash
# One-time setup: a pinned full Apache Spark distribution, a
# virtualenv with the Python client dependencies, Delta Lake jars,
# and the Spark confs — all inside this repository, nothing global.
# Run from the repository root:  ./setup.sh
# Requires: Java 17+ on PATH, Python 3.10+ (pyenv-managed is fine).
set -euo pipefail
cd "$(dirname "$0")"

SPARK_VERSION=4.2.0
DELTA_VERSION=4.4.0
DIST="spark-${SPARK_VERSION}-bin-hadoop3"

if [ ! -d spark ]; then
  echo "==> Downloading pinned Apache Spark ${SPARK_VERSION} (~400 MB, once)"
  curl -fL --progress-bar \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${DIST}.tgz" \
    -o "${DIST}.tgz"
  tar -xzf "${DIST}.tgz"
  mv "${DIST}" spark
  rm "${DIST}.tgz"
else
  echo "==> spark/ already present, skipping download"
fi

echo "==> Adding Delta Lake ${DELTA_VERSION} jars to spark/jars/"
for u in \
  "https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar" \
  "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"; do
  (cd spark/jars && curl -sfLO "$u")
done

echo "==> Writing spark/conf/spark-defaults.conf (Delta extensions)"
cat > spark/conf/spark-defaults.conf <<EOF
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
EOF

echo "==> Creating virtualenv with the Python client dependencies"
python3 -m venv .venv
.venv/bin/pip install --quiet \
  pyyaml pandas pyarrow grpcio grpcio-status googleapis-common-protos zstandard

echo "==> Writing .env so VS Code / Pylance resolves pyspark from spark/"
PY4J_ZIP="$(basename spark/python/lib/py4j-*.zip)"
cat > .env <<EOF
PYTHONPATH=\${workspaceFolder}/spark/python:\${workspaceFolder}/spark/python/lib/${PY4J_ZIP}
EOF

echo "==> Done. Next:  ./run.sh   then:  ./inspect.sh   or:  ./shell.sh"
