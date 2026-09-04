#!/usr/bin/env bash
# Run the pipeline with spark-pipelines from the shared Spark install.
# Extra arguments pass through, e.g.:  ./run.sh dry-run
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

# The spec's storage root must be an absolute file:// URI, so the
# committed template is rendered with this checkout's path.
sed "s|__REPO_ROOT__|$PWD|" pipeline/spark-pipeline.template.yml \
  > pipeline/spark-pipeline.yml

cd pipeline
exec "$SPARK_HOME/bin/spark-pipelines" "${1:-run}" --spec spark-pipeline.yml "${@:2}"
