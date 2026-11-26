#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Run on the MASTER only, ONCE. Formats the HDFS NameNode.
# WARNING: re-running this erases everything stored in HDFS.
# ---------------------------------------------------------------------
source "$(dirname "$0")/../common.sh"
require_master

if [[ -d "$HDFS_DATA_DIR/namenode/current" ]]; then
  read -r -p "HDFS is already formatted. Erase ALL HDFS data and re-format? [y/N] " ans
  [[ "$ans" == "y" ]] || { info "Skipped."; exit 0; }
  for host in "${WORKER_HOSTS[@]}"; do
    ssh "$CLUSTER_USER@$host" "rm -rf $HDFS_DATA_DIR/datanode/*"
  done
  rm -rf "$HDFS_DATA_DIR/namenode/"*
fi

hdfs namenode -format -force -nonInteractive
ok "HDFS formatted. Next: bash scripts/cluster/start_cluster.sh"
