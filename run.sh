#!/usr/bin/env bash
# Run the pipeline with the spark-pipelines CLI. Any extra arguments
# are passed through, e.g.:  ./run.sh dry-run
set -euo pipefail
cd "$(dirname "$0")"

export SPARK_HOME="$(.venv/bin/python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"

# The spec's storage root must be an absolute file:// URI, so the
# committed template is rendered with this checkout's path.
sed "s|__REPO_ROOT__|$PWD|" pipeline/spark-pipeline.template.yml \
  > pipeline/spark-pipeline.yml

cd pipeline
exec "../.venv/bin/spark-pipelines" "${1:-run}" --spec spark-pipeline.yml "${@:2}"
