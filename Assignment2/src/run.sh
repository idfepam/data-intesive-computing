#!/usr/bin/env bash

set -euo pipefail

# ---------------------------------------------------------
# Move to script directory
# ---------------------------------------------------------

SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SRC_DIR"

# ---------------------------------------------------------
# Input / output
# ---------------------------------------------------------

INPUT="${1:-hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json}"

OUTPUT="${2:-output_rdd}"

# ---------------------------------------------------------
# Remove old output if it exists
# ---------------------------------------------------------

hdfs dfs -rm -r -f "$OUTPUT" >/dev/null 2>&1 || true

# ---------------------------------------------------------
# Run Spark RDD pipeline
# ---------------------------------------------------------

echo "[run.sh] Input  : $INPUT"
echo "[run.sh] Output : $OUTPUT"

spark-submit \
    --master yarn \
    --deploy-mode client \
    --files stopwords.txt \
    chi_rdd.py \
    "$INPUT" \
    "$OUTPUT"

echo "[run.sh] Done."