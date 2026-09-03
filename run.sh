#!/usr/bin/env bash
# Run the pipeline with the distribution's spark-pipelines CLI.
# Extra arguments pass through, e.g.:  ./run.sh dry-run
set -euo pipefail
cd "$(dirname "$0")"

export SPARK_HOME="$PWD/spark"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"

# The spec's storage root must be an absolute file:// URI, so the
# committed template is rendered with this checkout's path.
sed "s|__REPO_ROOT__|$PWD|" pipeline/spark-pipeline.template.yml \
  > pipeline/spark-pipeline.yml

cd pipeline
exec "$SPARK_HOME/bin/spark-pipelines" "${1:-run}" --spec spark-pipeline.yml "${@:2}"
