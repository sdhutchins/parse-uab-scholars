#!/bin/bash
#SBATCH --job-name=uab_pipeline
#SBATCH --output=logs/slurm_chunk_%a.out
#SBATCH --error=logs/slurm_chunk_%a.err
#SBATCH --array=0-127
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=02:00:00
#SBATCH --partition=express,amd-hdr100
#
# Resource justification:
#   cpus-per-task=1  - API calls are I/O-bound, not CPU-bound.
#   mem=1G           - JSON parsing of small payloads; prior runs
#                      peak under 200 MB.
#   time=02:00:00    - Each chunk now fetches committees AND enhanced
#                      data. Prior single-stage jobs completed well
#                      under 1 hour; doubling gives headroom.
#   array=0-127      - ~5000 faculty / 128 chunks ≈ 40 per chunk.

set -euo pipefail

module load Python/3.11.5-GCCcore-13.2.0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export CHUNK_ID=${SLURM_ARRAY_TASK_ID:-0}
export CHUNK_TOTAL=128
export N_THREADS=1

LOGDIR="logs"
mkdir -p "$LOGDIR"

RETRY_REGISTRY="$LOGDIR/retry_registry_chunk_${CHUNK_ID}.csv"
export RETRY_REGISTRY

ITER=1
while true; do
    logger_prefix="[chunk $CHUNK_ID iter $ITER]"
    echo "$logger_prefix Starting"
    echo "$logger_prefix Retry registry: $RETRY_REGISTRY"

    python3 "$SCRIPT_DIR/fetch_uab_data.py"

    if [[ ! -s "$RETRY_REGISTRY" ]]; then
        echo "$logger_prefix No retryable errors remain"
        rm -f "$RETRY_REGISTRY"
        break
    fi

    echo "$logger_prefix Retrying — $(wc -l < "$RETRY_REGISTRY") IDs still failing"
    ((ITER++))
done

echo "[chunk $CHUNK_ID] Pipeline complete"
