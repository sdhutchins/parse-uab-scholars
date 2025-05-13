#!/bin/bash
#SBATCH --job-name=grad_committees
#SBATCH --output=logs/slurm_chunk_%a.out
#SBATCH --error=logs/slurm_chunk_%a.err
#SBATCH --array=0-127                  # Adjust based on CHUNK_TOTAL
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

# === Loop until no errors ===
ITER=1
while true; do
    echo "🚀 Starting iteration $ITER for chunk $CHUNK_ID"

    # Run fetch script — will skip existing JSONs
    python3 fetch_graduate_committee.py

    # If no error log created or it's empty, we're done
    if [ ! -s "$ERROR_LOG" ]; then
        echo "✅ No errors logged. Done with chunk $CHUNK_ID"
        break
    fi

    echo "🔁 Retrying chunk $CHUNK_ID (errors detected)"
    rm -f "$ERROR_LOG"
    ((ITER++))
done

echo "🎓 Completed all retries for chunk $CHUNK_ID"
