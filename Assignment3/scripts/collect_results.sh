#!/usr/bin/env bash
# collect_results.sh
# --------------------
# Uploads reviews_devset.json one review at a time and waits for the
# full pipeline (preprocessing -> profanity-check -> sentiment-analysis)
# to finish, then tallies the results.
#
# Usage:
#   ./scripts/collect_results.sh
#
# Env vars (all optional, sensible defaults below):
#   DEVSET           path to reviews_devset.json
#   FULL_TIMEOUT     seconds to wait for all reviews to finish processing
#                    after the upload loop completes
set -euo pipefail

cd "$(dirname "$0")/.."

DEVSET="${DEVSET:-data/reviews_devset.json}"
UPLOAD_SLEEP="${UPLOAD_SLEEP:-0.05}"
FULL_TIMEOUT="${FULL_TIMEOUT:-1800}"

if [ ! -f "$DEVSET" ]; then
  echo "ERROR: devset not found at $DEVSET (set DEVSET=... to override)" >&2
  exit 1
fi

echo "=== Step 1/2: uploading devset (${UPLOAD_SLEEP}s delay between reviews) ==="
python scripts/run_devset.py --devset "$DEVSET" --sleep "$UPLOAD_SLEEP"

echo
echo "=== Step 2/2: waiting for pipeline to finish ==="
python scripts/tally_results.py --devset "$DEVSET" --wait \
  --timeout "$FULL_TIMEOUT" --out data/results_summary.json

echo
echo "Done. Results: data/results_summary.json"
