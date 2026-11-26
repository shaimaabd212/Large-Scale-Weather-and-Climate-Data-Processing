#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Run on the MASTER only. Gives the master password-less SSH to itself
# and both workers (Hadoop and Spark start scripts need this).
# You will be asked for each machine's password once.
# ---------------------------------------------------------------------
source "$(dirname "$0")/../common.sh"
require_master

[[ -f ~/.ssh/id_rsa ]] || ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/id_rsa

for host in "${ALL_HOSTS[@]}"; do
  info "Copying key to $CLUSTER_USER@$host"
  ssh-copy-id -o StrictHostKeyChecking=accept-new "$CLUSTER_USER@$host"
done

for host in "${ALL_HOSTS[@]}"; do
  if ssh -o BatchMode=yes -o ConnectTimeout=5 "$CLUSTER_USER@$host" "hostname" >/dev/null; then
    ok "SSH to $host works without password"
  else
    fail "SSH to $host still asks for a password"
  fi
done
