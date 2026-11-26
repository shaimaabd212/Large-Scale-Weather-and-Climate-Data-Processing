# Work plan — 3 members, 1 master + 2 workers

Each member **hosts one node** of the cluster and **owns one layer** of the pipeline end-to-end:
the code, its tests on the sample data, its section of the report, and its part of the demo.

| | Member A | Member B | Member C |
|---|---|---|---|
| **Machine** | `bd-master` (NameNode, Spark Master, driver) | `bd-worker1` (DataNode, Worker) | `bd-worker2` (DataNode, Worker) |
| **Role** | Infrastructure & integration lead | Data engineering lead | Analytics & reporting lead |
| **Owns** | cluster, HDFS, job running, performance | getting data in and making it trustworthy | turning clean data into climate findings |
| **Report sections** | 3 Architecture, 6 Performance & scaling | 4 Data & ingestion, 5 Cleaning & quality | 7 Results, 8 Descriptive vs causal, 9 Conclusion |

## Who owns which files

| Member A — infrastructure | Member B — data engineering | Member C — analytics |
|---|---|---|
| `config/cluster.env` | `config/pipeline.json` (ranges, completeness, elements) | `config/pipeline.json` (thresholds, baseline, grid, focus) |
| `config/hadoop/*`, `config/spark/*` | `scripts/data/download_ghcnd.sh` | `src/weather_pipeline/extremes.py` |
| `scripts/setup/*`, `scripts/cluster/*` | `scripts/data/upload_to_hdfs.sh` | `src/weather_pipeline/spatial.py` |
| `scripts/run_pipeline.sh`, `scripts/benchmark.sh` | `scripts/data/make_sample_data.py` | `src/weather_pipeline/export.py` |
| `pipelines/run_pipeline.py` | `src/weather_pipeline/ingest.py` | `notebooks/01_climate_analysis.ipynb` |
| `src/weather_pipeline/spark_session.py`, `config.py` | `src/weather_pipeline/clean.py` | `docs/REPORT_OUTLINE.md` + final report assembly |
| `docs/SETUP_GUIDE.md` | `src/weather_pipeline/aggregate.py` | slides |

Everyone sets up their **own** machine with `01_install_node.sh` + `02_configure_node.sh`.

## Six-week timeline

| Week | Member A (master) | Member B (worker 1) | Member C (worker 2) | Milestone (end of week) |
|---|---|---|---|---|
| **1 — Setup** | Fill `cluster.env`, set up GitHub branches; install master node | Read GHCN readme; run sample generator; install worker 1 | Read the brief + ETCCDI index definitions; install worker 2 | All 3 run `run_pipeline.sh --local --sample` on their laptops |
| **2 — Cluster** | SSH, format HDFS, start cluster; screenshots of UIs | Download 2 test years; upload to HDFS; check block placement (`hdfs fsck`) | Review export CSVs from the sample run; first notebook plots | `status.sh` shows 2 live DataNodes + 2 Spark workers; 2-year run succeeds |
| **3 — Full data** | Tune executor memory / partitions for your hardware | Full download (10 yrs); ingest + clean on the cluster; quality numbers | Run aggregate/extremes/spatial on full data; sanity-check values | Full 10-year run finishes; `logs/run_*.json` saved |
| **4 — Depth** | Benchmark 1 vs 2 workers; Parquet vs CSV.gz read test; worker-failure test | Extension: statistical outlier check; TAVG vs (TMAX+TMIN)/2 comparison | Extension: heatwave detection; focus on Libya & North Africa | Each member has one extension working |
| **5 — Write-up** | Report §3, §6 + architecture diagram | Report §4, §5 + quality tables | Report §7, §8, §9 + all figures | Full report draft |
| **6 — Polish** | Rehearse the live demo (start cluster → run → UIs) | Proof-read; reproducibility test from a fresh clone | Assemble report + slides | Submit + present |

> **Less time?** If the deadline comes sooner, merge weeks 1+2 (setup and cluster in the same
> session with all three present), 3+4 (skip the extensions, keep the benchmark), and 5+6.

## Extension ideas (pick one each in week 4)

**Member A — show the "big data" part**
- Scaling: `scripts/benchmark.sh` (1 worker vs 2) → bar chart + speed-up in report.
- Format: time a query on raw `csv.gz` vs bronze Parquet (explain non-splittable gzip).
- Tuning: `SHUFFLE_PARTITIONS` 8 / 16 / 32 / 64, with and without AQE.
- Fault tolerance: stop a Worker mid-job (`stop-worker.sh` on worker 2) and show Spark re-runs the lost tasks; stop a DataNode and show HDFS still serves data (replication = 2).

**Member B — robust handling of missing & inconsistent records**
- Statistical outlier flag: day value more than 5 standard deviations from that station's
  baseline mean for that calendar month.
- Compare reported TAVG with (TMAX+TMIN)/2 at stations that have both — justify our choice.
- Use `ghcnd-inventory.txt` to keep only stations with records covering the full period.
- Cross-check Benghazi/Tripoli monthly means against the Open-Meteo archive (bounded sample).

**Member C — climate analysis**
- Heatwave detection: ≥ 3 consecutive days above the station's baseline 90th percentile of TMAX
  (windows + "gaps and islands", like `longest_spells()`).
- Country deep-dive on Libya / North Africa (note how few GHCN stations Libya has — itself a finding).
- Seasonal trend maps from `station_season_trends`.

## How we work together

- **Branches:** `main` is always runnable. Each member works on `feature/<name>-<topic>` and opens a
  pull request; one other member reviews it before merging.
- **Before every PR:** `bash scripts/run_pipeline.sh --local --sample` must finish without errors.
- **Interfaces between members are the tables**: B's `silver/daily` columns are the contract for C.
  If B renames or adds a column, B tells C in the PR description.
- **Weekly 30-minute sync** (same day every week): each member shows what ran, what broke, what's next.
- **Cluster sessions** need all three machines on: book 2 fixed slots per week where everyone
  brings their laptop to the same place.

## Definition of done (whole project)

- [ ] Cluster starts with one command and shows 2 DataNodes + 2 Workers
- [ ] 10 years of GHCN-Daily ingested to HDFS and processed end-to-end on the cluster
- [ ] Quality report: how many records were missing / failed QC / out of range / duplicated
- [ ] Monthly, seasonal, annual summaries; climatology and anomalies; extremes; spatial aggregates; trends
- [ ] 1-worker vs 2-worker benchmark with numbers
- [ ] Notebook figures + report with a clear "descriptive, not causal" section
- [ ] Anyone can reproduce from a fresh clone using `docs/SETUP_GUIDE.md`
