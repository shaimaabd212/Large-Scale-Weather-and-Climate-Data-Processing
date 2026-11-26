#!/usr/bin/env bash
# Run on the MASTER. Shows which daemons run where + HDFS and Spark health.
source "$(dirname "$0")/../common.sh"

echo "================ Java processes per node ================"
for host in "${ALL_HOSTS[@]}"; do
  echo "--- $host"
  ssh -o ConnectTimeout=5 "$CLUSTER_USER@$host" "source ~/.bashrc >/dev/null 2>&1; jps | grep -v Jps" \
    || warn "cannot reach $host"
done
echo
echo "Expected:  $MASTER_HOST -> NameNode, SecondaryNameNode, Master (+ HistoryServer)"
echo "           workers      -> DataNode, Worker"
echo
echo "================ HDFS report ================"
hdfs dfsadmin -report 2>/dev/null | grep -E "Live datanodes|^Name:|DFS Remaining:" || warn "HDFS not reachable"
echo
echo "================ Spark master ================"
if curl -s "http://$MASTER_HOST:8080/json/" > /tmp/spark_status.json; then
  python3 - <<'EOF'
import json
s = json.load(open("/tmp/spark_status.json"))
print(f"Status: {s.get('status')}   alive workers: {s.get('aliveworkers')}   cores: {s.get('cores')}   memory MB: {s.get('memory')}")
for w in s.get("workers", []):
    print(f"  - {w['host']:<15} {w['state']:<6} cores={w['cores']} mem={w['memory']}MB")
EOF
else
  warn "Spark master UI not reachable"
fi
echo
echo "Web UIs:  HDFS http://$MASTER_HOST:9870   Spark http://$MASTER_HOST:8080   History http://$MASTER_HOST:18080"
