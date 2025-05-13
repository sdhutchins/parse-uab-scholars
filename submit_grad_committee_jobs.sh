#!/bin/bash
#SBATCH --job-name=grad_committees
#SBATCH --output=logs/slurm_chunk_%a.out
#SBATCH --error=logs/slurm_chunk_%a.err
#SBATCH --array=0-127
#SBATCH --cpus-per-task=8
#SBATCH --mem=4G
#SBATCH --time=01:00:00
#SBATCH --partition=express,amd-hdr100

module load Python/3.11.5-GCCcore-13.2.0

# === Setup ===
export CHUNK_ID=${SLURM_ARRAY_TASK_ID}
export CHUNK_TOTAL=128
export N_THREADS=8

LOGDIR="logs"
ERROR_LOG="$LOGDIR/chunk_${CHUNK_ID}_grad_committee_errors.log"
RETRY_REGISTRY="$LOGDIR/retry_registry_chunk_${CHUNK_ID}.csv"
export RETRY_REGISTRY

ITER=1
while true; do
    echo "🚀 Starting iteration $ITER for chunk $CHUNK_ID"

    # Run the fetch script
    python3 fetch_graduate_committee.py

    # Check if retry registry was created and is non-empty
    if [ ! -s "$RETRY_REGISTRY" ]; then
        echo "✅ No retryable errors remain in chunk $CHUNK_ID"
        break
    fi

    echo "🔁 Retrying chunk $CHUNK_ID — still $(wc -l <"$RETRY_REGISTRY") IDs failing"
    ((ITER++))
done

echo "🎓 Completed all retries for chunk $CHUNK_ID"
