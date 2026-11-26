#!/usr/bin/env bash
# Run on the MASTER. Starts HDFS (NameNode + 2 DataNodes) and Spark (Master + 2 Workers).
source "$(dirname "$0")/../common.sh"
require_master

info "Starting HDFS"
start-dfs.sh

info "Waiting for HDFS to leave safe mode"
hdfs dfsadmin -safemode wait

hdfs dfs -mkdir -p /spark-logs "$HDFS_BASE"

info "Starting Spark master + workers"
"$SPARK_HOME/sbin/start-master.sh"
"$SPARK_HOME/sbin/start-workers.sh"
"$SPARK_HOME/sbin/start-history-server.sh" || warn "History server did not start (optional)."

sleep 5
bash "$(dirname "$0")/status.sh"
