#!/usr/bin/env bash
# Remove all local pipeline state (tables, metastore, checkpoints) for
# a clean slate. Source events and code are untouched.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf pipeline/spark-warehouse pipeline/metastore_db \
  pipeline/pipeline-storage pipeline/derby.log
echo "Pipeline state removed. Next run starts fresh."
