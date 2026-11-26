#!/usr/bin/env bash
# Run on the MASTER. Stops Spark and HDFS on all nodes.
source "$(dirname "$0")/../common.sh"
require_master

"$SPARK_HOME/sbin/stop-history-server.sh" || true
"$SPARK_HOME/sbin/stop-workers.sh" || true
"$SPARK_HOME/sbin/stop-master.sh" || true
stop-dfs.sh || true
ok "Cluster stopped."
