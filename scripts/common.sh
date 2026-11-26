#!/usr/bin/env bash
# Shared helpers. Sourced by every other script: `source "$(dirname "$0")/../common.sh"`
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_DIR="$REPO_ROOT/config"

# shellcheck disable=SC1091
source "$CONFIG_DIR/cluster.env"

ALL_HOSTS=("$MASTER_HOST" "$WORKER1_HOST" "$WORKER2_HOST")
WORKER_HOSTS=("$WORKER1_HOST" "$WORKER2_HOST")

export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
ok()    { echo -e "\033[1;32m[ OK ]\033[0m  $*"; }
warn()  { echo -e "\033[1;33m[WARN]\033[0m  $*"; }
fail()  { echo -e "\033[1;31m[FAIL]\033[0m  $*" >&2; exit 1; }

require_master() {
  [[ "$(hostname)" == "$MASTER_HOST" ]] || fail "Run this on the master ($MASTER_HOST), not on $(hostname)."
}

detect_java_home() {
  local javac_path
  javac_path="$(command -v javac || true)"
  [[ -n "$javac_path" ]] || fail "javac not found — run scripts/setup/01_install_node.sh first."
  dirname "$(dirname "$(readlink -f "$javac_path")")"
}
