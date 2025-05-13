#!/bin/bash
#SBATCH --job-name=grad_committees
#SBATCH --output=logs/slurm_chunk_%a.out
#SBATCH --error=logs/slurm_chunk_%a.err
#SBATCH --array=0-127                  # ⬅️ 128 jobs = ~50 users per chunk (6400 / 128)
#SBATCH --cpus-per-task=8              # ⬅️ Use 8 threads per task
#SBATCH --mem=4G
#SBATCH --time=01:00:00
#SBATCH --partition=express,amd-hdr100

module load Python/3.11.5-GCCcore-13.2.0

export CHUNK_ID=${SLURM_ARRAY_TASK_ID}
export CHUNK_TOTAL=128
export N_THREADS=8

python fetch_grad_committee.py
