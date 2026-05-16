#!/usr/bin/env bash
#
# End-to-end driver for the TU Wien Hadoop cluster (lbd.tuwien.ac.at).
#
# Usage:  bash run.sh [HDFS_INPUT] [LOCAL_OUTPUT]
#
# Defaults match the assignment:
#   HDFS_INPUT  = hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json
#   LOCAL_OUTPUT= ../output.txt
#
# The script runs two mrjob jobs on Hadoop and a small local post-process
# in between that builds the meta.tsv side-input (<= a few hundred bytes).

set -euo pipefail

SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SRC_DIR"

INPUT="${1:-hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json}"
OUTPUT_LOCAL="${2:-../output.txt}"

# Unique HDFS working dirs (so re-runs do not clash).
STAMP="$(date +%s)_$$"
COUNT_OUT="hdfs:///user/${USER:-$(whoami)}/dic_a1_${STAMP}/counts"
STATS_OUT="hdfs:///user/${USER:-$(whoami)}/dic_a1_${STAMP}/stats"
META_LOCAL="meta.tsv"

echo "[run.sh] Input        : $INPUT"
echo "[run.sh] Count output : $COUNT_OUT"
echo "[run.sh] Stats output : $STATS_OUT"

# ---------------------------------------------------------------------------
# Stage 1: counts
# ---------------------------------------------------------------------------
echo "[run.sh] Stage 1: chi_count.py"
python3 chi_count.py \
    -r hadoop \
    --file stopwords.txt \
    --output-dir "$COUNT_OUT" \
    --no-output \
    "$INPUT"

# ---------------------------------------------------------------------------
# Extract meta (n + n_c) into a small local file.  We only pull the `c` and
# `n` records from HDFS (they are tiny compared with the term-level output).
# ---------------------------------------------------------------------------
echo "[run.sh] Building $META_LOCAL"
hdfs dfs -cat "$COUNT_OUT/part-*" \
  | python3 -c "
import json, sys
cat = {}
N = 0
for raw in sys.stdin:
    k, _, v = raw.rstrip('\n').partition('\t')
    if not v: continue
    try:
        key = json.loads(k); val = int(json.loads(v))
    except Exception:
        continue
    if not isinstance(key, list) or not key: continue
    if key[0] == 'c' and len(key) >= 2:
        cat[key[1]] = cat.get(key[1], 0) + val
    elif key[0] == 'n':
        N += val
with open('$META_LOCAL', 'w', encoding='utf-8') as f:
    f.write('n\t%d\n' % N)
    for c in sorted(cat):
        f.write('c\t%s\t%d\n' % (c, cat[c]))
print('[run.sh] N=%d categories=%d' % (N, len(cat)), file=sys.stderr)
"

# ---------------------------------------------------------------------------
# Stage 2: chi-square + top-75 + merge
# ---------------------------------------------------------------------------
echo "[run.sh] Stage 2: chi_stats.py"
python3 chi_stats.py \
    -r hadoop \
    --file "$META_LOCAL" \
    --meta meta.tsv \
    --output-dir "$STATS_OUT" \
    --no-output \
    "$COUNT_OUT"

# ---------------------------------------------------------------------------
# Pull final output and concatenate.  Stage 2's last step uses a single
# reducer, so the order of lines is already correct.
# ---------------------------------------------------------------------------
echo "[run.sh] Writing $OUTPUT_LOCAL"
hdfs dfs -cat "$STATS_OUT/part-*" > "$OUTPUT_LOCAL"

echo "[run.sh] Done."