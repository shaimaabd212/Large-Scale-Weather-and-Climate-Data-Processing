#!/usr/bin/env bash
# Template: rendered by scripts/setup/02_configure_node.sh
export JAVA_HOME=${JAVA_HOME}
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export SPARK_MASTER_HOST=${MASTER_HOST}
export SPARK_WORKER_CORES=${WORKER_CORES}
export SPARK_WORKER_MEMORY=${WORKER_MEMORY}
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
