#!/bin/bash
#SBATCH --job-name=grad_committees
#SBATCH --output=logs/slurm_chunk_%a.out
#SBATCH --error=logs/slurm_chunk_%a.err
#SBATCH --array=0-127                  # 128 jobs = ~50 users per chunk
#SBATCH --cpus-per-task=8
#SBATCH --mem=4G
#SBATCH --time=01:00:00
#SBATCH --partition=express,amd-hdr100

module load Python/3.11.5-GCCcore-13.2.0

# === Setup ===
export CHUNK_ID=${SLURM_ARRAY_TASK_ID}
export CHUNK_TOTAL=128
export N_THREADS=8

# Working files
LOGDIR="logs"
ERROR_LOG="$LOGDIR/chunk_${CHUNK_ID}_grad_committee_errors_main.log"
RETRY_REGISTRY="logs/retry_registry_chunk_${CHUNK_ID}.csv"

# === Loop until no errors ===
ITER=1
while true; do
    echo "🚀 Starting iteration $ITER for chunk $CHUNK_ID"

    # Clear retry file
    rm -f "$RETRY_REGISTRY"

    # Run fetch (uses retry file if it exists)
    export RETRY_REGISTRY
    python3 fetch_graduate_committee.py

    # Check if new errors were logged for this chunk
    if [ ! -f "$ERROR_LOG" ]; then
        echo "✅ No error log found. Done with chunk $CHUNK_ID"
        break
    fi

    cut -d',' -f1 "$ERROR_LOG" | sort -u >"$RETRY_REGISTRY"

    COUNT=$(wc -l <"$RETRY_REGISTRY")
    if [ "$COUNT" -eq 0 ]; then
        echo "✅ No retryable IDs in chunk $CHUNK_ID"
        break
    else
        echo "🔁 Retrying $COUNT IDs in chunk $CHUNK_ID"
        # Clear error log before next run
        rm -f "$ERROR_LOG"
        ((ITER++))
    fi
done

echo "🎓 Completed all retries for chunk $CHUNK_ID"
