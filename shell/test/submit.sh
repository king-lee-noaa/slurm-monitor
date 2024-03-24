#!/bin/bash
#
#SBATCH --job-name=test
#SBATCH --output=test-output.txt
#SBATCH --partition=dev
#
#SBATCH --time=10:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=100

srun hostname
srun test.sh
