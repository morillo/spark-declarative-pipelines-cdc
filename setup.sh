#!/usr/bin/env bash
# One-time setup: virtualenv, exact package versions, Delta jars, and
# the static Spark confs. Run from the repository root:  ./setup.sh
# Requires: Java 17+ on PATH, Python 3.10+ (pyenv-managed is fine).
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Creating virtualenv (.venv)"
python3 -m venv .venv

echo "==> Installing pinned packages (see README for why these exact versions)"
.venv/bin/pip install --quiet \
  "pyspark[connect]==4.2.0" \
  "delta-spark==4.4.0" \
  pyyaml

echo "==> Downloading Delta Lake jars from Maven Central"
mkdir -p jars
for u in \
  "https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.4.0/delta-spark_2.13-4.4.0.jar" \
  "https://repo1.maven.org/maven2/io/delta/delta-storage/4.4.0/delta-storage-4.4.0.jar" \
  "https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.13.1/antlr4-runtime-4.13.1.jar"; do
  (cd jars && curl -sfLO "$u")
done

echo "==> Writing static Spark confs (spark.jars cannot be set per-session)"
SPARK_HOME="$(.venv/bin/python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
mkdir -p "$SPARK_HOME/conf"
cat > "$SPARK_HOME/conf/spark-defaults.conf" <<EOF
spark.jars $PWD/jars/delta-spark_2.13-4.4.0.jar,$PWD/jars/delta-storage-4.4.0.jar,$PWD/jars/antlr4-runtime-4.13.1.jar
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
EOF

echo "==> Done. Next:  ./run.sh   then:  ./inspect.sh"
