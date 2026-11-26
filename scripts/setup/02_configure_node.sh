#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Run on EVERY machine after 01_install_node.sh (and again whenever you
# change config/cluster.env). Renders the templates in config/hadoop and
# config/spark into $HADOOP_HOME/etc/hadoop and $SPARK_HOME/conf.
# ---------------------------------------------------------------------
source "$(dirname "$0")/../common.sh"

export JAVA_HOME
JAVA_HOME="$(detect_java_home)"
export MASTER_HOST HDFS_DATA_DIR HDFS_REPLICATION EXECUTOR_MEMORY DRIVER_MEMORY \
       SHUFFLE_PARTITIONS WORKER_CORES WORKER_MEMORY HADOOP_HOME

# Only these variables get substituted (so other $ signs survive)
VARS='${MASTER_HOST} ${HDFS_DATA_DIR} ${HDFS_REPLICATION} ${EXECUTOR_MEMORY} ${DRIVER_MEMORY} ${SHUFFLE_PARTITIONS} ${WORKER_CORES} ${WORKER_MEMORY} ${JAVA_HOME} ${HADOOP_HOME}'

HCONF="$HADOOP_HOME/etc/hadoop"
SCONF="$SPARK_HOME/conf"

info "Rendering Hadoop config -> $HCONF"
for f in core-site.xml hdfs-site.xml; do
  envsubst "$VARS" < "$CONFIG_DIR/hadoop/$f" > "$HCONF/$f"
done
printf "%s\n" "${WORKER_HOSTS[@]}" > "$HCONF/workers"
sed -i '/^export JAVA_HOME=/d' "$HCONF/hadoop-env.sh"
echo "export JAVA_HOME=$JAVA_HOME" >> "$HCONF/hadoop-env.sh"

info "Rendering Spark config -> $SCONF"
envsubst "$VARS" < "$CONFIG_DIR/spark/spark-defaults.conf" > "$SCONF/spark-defaults.conf"
envsubst "$VARS" < "$CONFIG_DIR/spark/spark-env.sh"        > "$SCONF/spark-env.sh"
chmod +x "$SCONF/spark-env.sh"
printf "%s\n" "${WORKER_HOSTS[@]}" > "$SCONF/workers"

mkdir -p "$HDFS_DATA_DIR"/{namenode,datanode,tmp}

ok "Configured $(hostname). Master: $MASTER_HOST | Workers: ${WORKER_HOSTS[*]}"
