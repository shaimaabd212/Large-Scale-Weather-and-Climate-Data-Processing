#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Scaling experiment for the report: run the same stages with 1 worker's worth
# of executors vs. both workers, and record the times in logs/benchmark.csv.
#   Usage (master): bash scripts/benchmark.sh [stages] [repeats]
#   Default stages: clean,aggregate,extremes,spatial   repeats: 2
# Needs bronze data to exist (run the ingest stage once first).
# ---------------------------------------------------------------------
source "$(dirname "$0")/common.sh"
require_master

STAGES="${1:-clean,aggregate,extremes,spatial}"
REPEATS="${2:-2}"
OUT="$REPO_ROOT/logs/benchmark.csv"
mkdir -p "$REPO_ROOT/logs"
[[ -f "$OUT" ]] || echo "timestamp,workers,total_cores,stages,run,seconds" > "$OUT"

for workers in 1 2; do
  cores=$(( WORKER_CORES * workers ))
  for run in $(seq 1 "$REPEATS"); do
    info "workers=$workers cores=$cores run=$run"
    start=$(date +%s)
    # One executor holds all of a worker's cores, so cores.max decides how many workers are used.
    SPARK_EXTRA="--conf spark.executor.cores=$WORKER_CORES --conf spark.cores.max=$cores" \
      bash "$REPO_ROOT/scripts/run_pipeline.sh" --stage "$STAGES" --tag "${workers}-worker"
    end=$(date +%s)
    echo "$(date -Iseconds),$workers,$cores,\"$STAGES\",$run,$((end - start))" >> "$OUT"
  done
done

ok "Results:"
column -s, -t < "$OUT"
