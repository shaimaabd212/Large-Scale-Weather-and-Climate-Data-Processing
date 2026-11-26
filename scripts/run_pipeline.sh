#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Runs the whole pipeline (or chosen stages).
#
#   On the cluster (run on the master):
#     bash scripts/run_pipeline.sh                       # all stages, data in HDFS
#     bash scripts/run_pipeline.sh --stage clean,aggregate
#     bash scripts/run_pipeline.sh --count               # also record row counts
#
#   On a laptop for development (needs: pip install pyspark):
#     python3 scripts/data/make_sample_data.py
#     bash scripts/run_pipeline.sh --local --sample      # tiny synthetic data
#     bash scripts/run_pipeline.sh --local               # real files in data/raw
#
# Extra spark-submit options can be passed with SPARK_EXTRA, e.g.
#     SPARK_EXTRA="--conf spark.cores.max=2" bash scripts/run_pipeline.sh
# Results: data/export/*.csv (cluster: copied out of HDFS), metrics: logs/run_*.json
# ---------------------------------------------------------------------
source "$(dirname "$0")/common.sh"

MODE=cluster; DATA_DIR="$REPO_ROOT/data"; PY_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --local)  MODE=local ;;
    --sample) DATA_DIR="$REPO_ROOT/data/sample" ;;
    --stage)  PY_ARGS+=(--stage "$2"); shift ;;
    --count)  PY_ARGS+=(--count) ;;
    --tag)    PY_ARGS+=(--tag "$2"); shift ;;
    *) fail "Unknown option $1" ;;
  esac
  shift
done

cd "$REPO_ROOT"

if [[ "$MODE" == "local" ]]; then
  [[ -d "$DATA_DIR/raw/by_year" ]] || fail "No data in $DATA_DIR/raw — run make_sample_data.py or download_ghcnd.sh"
  info "LOCAL mode, data: $DATA_DIR"
  python3 pipelines/run_pipeline.py --local --base "file://$DATA_DIR" "${PY_ARGS[@]}"
  ok "Exports in $DATA_DIR/export/"
  exit 0
fi

require_master
BASE="hdfs://$MASTER_HOST:9000$HDFS_BASE"
info "CLUSTER mode, master spark://$MASTER_HOST:7077, data $BASE"

mkdir -p build
rm -f build/weather_pipeline.zip
(cd src && python3 -m zipfile -c ../build/weather_pipeline.zip weather_pipeline)

# shellcheck disable=SC2086
spark-submit \
  --master "spark://$MASTER_HOST:7077" \
  --deploy-mode client \
  --py-files build/weather_pipeline.zip \
  ${SPARK_EXTRA:-} \
  pipelines/run_pipeline.py --base "$BASE" "${PY_ARGS[@]}"

if hdfs dfs -test -d "$HDFS_BASE/export"; then
  info "Copying exports from HDFS to data/export/"
  rm -rf data/export && mkdir -p data/export
  hdfs dfs -get "$HDFS_BASE/export/"* data/export/
  ok "Exports in data/export/ — open notebooks/01_climate_analysis.ipynb"
fi
