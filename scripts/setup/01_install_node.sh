#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Run on EVERY machine (master, worker1, worker2), once.
#   Usage:  bash scripts/setup/01_install_node.sh master|worker1|worker2
# Installs Java, Python, Hadoop and Spark, sets the hostname and /etc/hosts.
# Tested target: Ubuntu 22.04 / 24.04 (VM with *bridged* networking or real machines).
# ---------------------------------------------------------------------
source "$(dirname "$0")/../common.sh"

ROLE="${1:-}"
case "$ROLE" in
  master)  NEW_HOSTNAME="$MASTER_HOST" ;;
  worker1) NEW_HOSTNAME="$WORKER1_HOST" ;;
  worker2) NEW_HOSTNAME="$WORKER2_HOST" ;;
  *) fail "Usage: $0 master|worker1|worker2" ;;
esac

[[ "$(whoami)" == "$CLUSTER_USER" ]] || fail "Log in as '$CLUSTER_USER' (CLUSTER_USER in config/cluster.env), not '$(whoami)'."

info "1/5 Installing system packages (needs sudo)"
sudo apt-get update -y
sudo apt-get install -y "$JAVA_PKG" python3 python3-pip openssh-server openssh-client \
  rsync wget curl gettext-base net-tools

info "2/5 Setting hostname to $NEW_HOSTNAME"
sudo hostnamectl set-hostname "$NEW_HOSTNAME"

info "3/5 Writing cluster hosts into /etc/hosts"
# Ubuntu maps the hostname to 127.0.1.1, which makes the NameNode/Spark master
# listen on loopback only. Comment that line out.
sudo sed -i 's/^127\.0\.1\.1/# 127.0.1.1/' /etc/hosts
sudo sed -i '/# bigdata-cluster$/d' /etc/hosts
{
  echo "$MASTER_IP  $MASTER_HOST   # bigdata-cluster"
  echo "$WORKER1_IP $WORKER1_HOST  # bigdata-cluster"
  echo "$WORKER2_IP $WORKER2_HOST  # bigdata-cluster"
} | sudo tee -a /etc/hosts >/dev/null

info "4/5 Downloading Hadoop $HADOOP_VERSION and Spark $SPARK_VERSION into /opt"
cd /tmp
HADOOP_TGZ="hadoop-${HADOOP_VERSION}.tar.gz"
SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
[[ -f "$HADOOP_TGZ" ]] || wget -c "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_TGZ}"
[[ -f "$SPARK_TGZ"  ]] || wget -c "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"

sudo tar -xzf "$HADOOP_TGZ" -C /opt
sudo tar -xzf "$SPARK_TGZ"  -C /opt
sudo ln -sfn "/opt/hadoop-${HADOOP_VERSION}"              "$HADOOP_HOME"
sudo ln -sfn "/opt/spark-${SPARK_VERSION}-bin-hadoop3"     "$SPARK_HOME"
sudo mkdir -p "$HDFS_DATA_DIR"
sudo chown -R "$CLUSTER_USER":"$CLUSTER_USER" "/opt/hadoop-${HADOOP_VERSION}" \
  "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "$HDFS_DATA_DIR"

info "5/5 Adding environment variables to ~/.bashrc"
JAVA_HOME_DETECTED="$(detect_java_home)"
sed -i '/# >>> bigdata-cluster >>>/,/# <<< bigdata-cluster <<</d' ~/.bashrc
cat >> ~/.bashrc <<EOF
# >>> bigdata-cluster >>>
export JAVA_HOME=$JAVA_HOME_DETECTED
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$SPARK_HOME
export PYSPARK_PYTHON=python3
export PATH=\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH
# <<< bigdata-cluster <<<
EOF

ok "Node '$NEW_HOSTNAME' installed. Next: bash scripts/setup/02_configure_node.sh (on this same machine)."
warn "Open a new terminal (or run: source ~/.bashrc) so the new PATH is active."
