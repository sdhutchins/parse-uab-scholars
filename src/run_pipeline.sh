#!/bin/bash
# Entry point for the full data collection pipeline.
#
# Usage (from the repo root or any directory):
#   bash src/run_pipeline.sh
#
# 1. Fetches all faculty profiles (sequential, runs on login node).
# 2. Submits the SLURM array job for committee + enhanced data.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"
mkdir -p logs

echo "=== Stage 1: Fetching faculty profiles ==="
FETCH_PROFILES=1 python3 "$SCRIPT_DIR/fetch_uab_data.py"

PROFILES="data/uab_scholars_profiles.jsonl"
if [[ ! -s "$PROFILES" ]]; then
    echo "ERROR: $PROFILES is empty or missing. Aborting."
    exit 1
fi

TOTAL=$(wc -l < "$PROFILES")
echo "Wrote $TOTAL profiles to $PROFILES"

echo ""
echo "=== Stage 2: Submitting SLURM array job ==="
sbatch "$SCRIPT_DIR/submit_pipeline.sh"
