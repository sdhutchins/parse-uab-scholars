#!/bin/bash
#SBATCH --job-name=uab_pipeline
#SBATCH --output=logs/slurm_%a_%A.out
#SBATCH --error=logs/slurm_%a_%A.err
#SBATCH --array=0-199
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=01:00:00
#SBATCH --partition=express,amd-hdr100

module load Python/3.11.5-GCCcore-13.2.0

# === Setup ===
export CHUNK_ID=${SLURM_ARRAY_TASK_ID:-0}
export CHUNK_TOTAL=200
export N_THREADS=1

LOGDIR="logs"

# === Stage 1: Fetch all profiles (run once) ===
if [ "$CHUNK_ID" -eq 0 ]; then
    echo "🚀 Stage 1: Fetching all UAB scholar profiles"
    export PIPELINE_STAGE=profiles
    python3 fetch_uab_data_pipeline.py
    echo "✅ Stage 1 complete"
fi

# === Stage 2: Enhanced data (parallel) ===
echo "🚀 Stage 2: Enhanced data for chunk $CHUNK_ID"
export PIPELINE_STAGE=enhanced
RETRY_REGISTRY="$LOGDIR/retry_registry_enhanced_chunk_${CHUNK_ID}.csv"
export RETRY_REGISTRY

ITER=1
while true; do
    echo "📄 Enhanced data iteration $ITER for chunk $CHUNK_ID"
    python3 fetch_uab_data_pipeline.py
    
    if [ ! -s "$RETRY_REGISTRY" ]; then
        echo "✅ No retryable errors remain in enhanced data chunk $CHUNK_ID"
        rm -f "$RETRY_REGISTRY"
        break
    fi
    
    echo "🔁 Retrying enhanced data chunk $CHUNK_ID — still $(wc -l <"$RETRY_REGISTRY") IDs failing"
    ((ITER++))
done

# === Stage 3: Graduate committees (parallel) ===
echo "🚀 Stage 3: Graduate committees for chunk $CHUNK_ID"
export PIPELINE_STAGE=committees
RETRY_REGISTRY="$LOGDIR/retry_registry_committees_chunk_${CHUNK_ID}.csv"
export RETRY_REGISTRY

ITER=1
while true; do
    echo "📄 Committees iteration $ITER for chunk $CHUNK_ID"
    python3 fetch_uab_data_pipeline.py
    
    if [ ! -s "$RETRY_REGISTRY" ]; then
        echo "✅ No retryable errors remain in committees chunk $CHUNK_ID"
        rm -f "$RETRY_REGISTRY"
        break
    fi
    
    echo "🔁 Retrying committees chunk $CHUNK_ID — still $(wc -l <"$RETRY_REGISTRY") IDs failing"
    ((ITER++))
done

echo "🎓 Completed all stages for chunk $CHUNK_ID" 