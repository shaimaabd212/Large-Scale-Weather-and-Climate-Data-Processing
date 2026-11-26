#!/usr/bin/env bash
# Run on the MASTER after download_ghcnd.sh. Copies data/raw into HDFS.
source "$(dirname "$0")/../common.sh"
require_master

RAW="$REPO_ROOT/data/raw"
hdfs dfs -mkdir -p "$HDFS_BASE/raw/by_year" "$HDFS_BASE/raw/meta"

info "Uploading metadata"
hdfs dfs -put -f "$RAW"/meta/ghcnd-*.txt "$HDFS_BASE/raw/meta/"

info "Uploading yearly files (skips files already in HDFS)"
for f in "$RAW"/by_year/*.csv.gz; do
  name="$(basename "$f")"
  if hdfs dfs -test -e "$HDFS_BASE/raw/by_year/$name"; then
    ok "$name already in HDFS"
  else
    hdfs dfs -put "$f" "$HDFS_BASE/raw/by_year/" && ok "$name uploaded"
  fi
done

hdfs dfs -du -h "$HDFS_BASE/raw"
info "Block placement across the two DataNodes (for the report):"
hdfs fsck "$HDFS_BASE/raw/by_year" -files -blocks -locations 2>/dev/null | grep -E "Total size|Total blocks|Average block replication" || true
