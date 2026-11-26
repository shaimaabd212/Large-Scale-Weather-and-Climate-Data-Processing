# Cluster setup guide — 1 master + 2 workers

Time needed: ~1.5 hours the first time, with all three people present.

## 0. Before you start

**Machines.** Three computers (or VMs) running **Ubuntu 22.04 or 24.04**, each with at least
8 GB RAM (a VM can have 4–6 GB) and 40 GB free disk.

* Windows laptops: install VirtualBox/VMware, create an Ubuntu VM and set its network adapter to
  **Bridged** (not NAT) so the three VMs can see each other on the LAN. WSL2 is *not*
  recommended for a multi-machine cluster (its networking is NAT-ed).
* If only one strong PC (16 GB+) is available, you can run all three VMs on it — the steps are identical.

**Network.** All three on the **same network**. University Wi-Fi often blocks device-to-device
traffic ("client isolation") — if `ping` between machines fails, use a phone hotspot or a small router.
Ask the router for fixed IPs (DHCP reservation) or the IPs may change between sessions.

**Same username everywhere.** Create the same Linux user on all three machines
(default in `config/cluster.env`: `bigdata`):
```bash
sudo adduser bigdata && sudo usermod -aG sudo bigdata   # then log in as bigdata
```

**Firewall.** Spark executors talk back to the driver on random ports. Inside your private
network, allow all traffic from the subnet:
```bash
sudo ufw allow from 192.168.1.0/24     # use your own subnet
```

## 1. Edit the config (once, Member A) and push

On each machine run `ip a` and note the IP. Put the three IPs in `config/cluster.env`,
adjust `WORKER_CORES` / `WORKER_MEMORY` to your hardware, commit and push. Everyone then:
```bash
git clone <repo-url> ~/weather-bigdata && cd ~/weather-bigdata
```

## 2. Install every node (each member on their own machine)

```bash
bash scripts/setup/01_install_node.sh master     # on the master
bash scripts/setup/01_install_node.sh worker1    # on worker 1
bash scripts/setup/01_install_node.sh worker2    # on worker 2
source ~/.bashrc
bash scripts/setup/02_configure_node.sh           # on all three
```
Check: `ping bd-master`, `ping bd-worker1`, `ping bd-worker2` must all work from every machine.

## 3. Master only

```bash
bash scripts/setup/03_setup_ssh.sh      # asks each password once
bash scripts/setup/04_format_hdfs.sh    # ONCE — re-running erases HDFS
bash scripts/cluster/start_cluster.sh
```
`status.sh` (run automatically) should show:

| Node | Processes (`jps`) |
|---|---|
| bd-master | NameNode, SecondaryNameNode, Master, HistoryServer |
| bd-worker1 / bd-worker2 | DataNode, Worker |

Web UIs (open from any of the three machines):
HDFS `http://bd-master:9870` · Spark `http://bd-master:8080` · History `http://bd-master:18080`.
Take screenshots of these for the report.

## 4. Data

```bash
bash scripts/data/download_ghcnd.sh 2022 2023   # start with 2 years to test
bash scripts/data/upload_to_hdfs.sh
bash scripts/run_pipeline.sh --count            # first full run
# then the full range from config/cluster.env:
bash scripts/data/download_ghcnd.sh && bash scripts/data/upload_to_hdfs.sh
bash scripts/run_pipeline.sh --count
bash scripts/benchmark.sh                       # 1 worker vs 2 workers
```
Results land in `data/export/`; run metrics in `logs/`.

## 5. Every session

```bash
bash scripts/cluster/start_cluster.sh   # master, after all 3 machines are on
...
bash scripts/cluster/stop_cluster.sh    # before shutting down
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `status.sh` shows 0 live DataNodes | Check `/etc/hosts` has no `127.0.1.1 bd-master` line; `ufw` rules; then `stop_cluster.sh` + `start_cluster.sh` |
| DataNode dies after re-formatting HDFS | Cluster ID mismatch: run `04_format_hdfs.sh` again and answer `y` (it wipes worker data too) |
| Spark UI shows workers but jobs hang at 0 tasks | Executors can't reach the driver → firewall; or `WORKER_MEMORY` is smaller than `EXECUTOR_MEMORY` |
| `java.lang.OutOfMemoryError` | Raise `SHUFFLE_PARTITIONS` (16→32) so each task holds less data, or raise `EXECUTOR_MEMORY` |
| Download is slow or fails | Script resumes on re-run, and falls back to the AWS mirror automatically |
| IPs changed after a reboot | Update `config/cluster.env` and the 3 lines ending in `# bigdata-cluster` in `/etc/hosts` on every node |
| Logs | `$HADOOP_HOME/logs/` and `$SPARK_HOME/logs/` on each machine |
