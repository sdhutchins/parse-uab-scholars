#!/bin/bash
#SBATCH --job-name=enhanced_data
#SBATCH --output=logs/slurm_chunk_%a.out
#SBATCH --error=logs/slurm_chunk_%a.err
#SBATCH --array=0-127
#SBATCH --cpus-per-task=8
#SBATCH --mem=4G
#SBATCH --time=01:00:00
#SBATCH --partition=express,amd-hdr100

module load Python/3.11.5-GCCcore-13.2.0

# === Setup ===
export CHUNK_ID=${SLURM_ARRAY_TASK_ID:-0}
export CHUNK_TOTAL=128
export N_THREADS=8

LOGDIR="logs"
RETRY_REGISTRY="$LOGDIR/retry_registry_enhanced_chunk_${CHUNK_ID}.csv"
export RETRY_REGISTRY

ITER=1
while true; do
    echo "🚀 Starting iteration $ITER for chunk $CHUNK_ID"
    echo "📄 Using retry registry: $RETRY_REGISTRY"

    python3 fetch_enhanced_data.py

    if [ ! -s "$RETRY_REGISTRY" ]; then
        echo "✅ No retryable errors remain in chunk $CHUNK_ID"
        rm -f "$RETRY_REGISTRY"
        break
    fi

    echo "🔁 Retrying chunk $CHUNK_ID — still $(wc -l <"$RETRY_REGISTRY") IDs failing"
    ((ITER++))
done

echo "🎓 Completed all retries for chunk $CHUNK_ID" 