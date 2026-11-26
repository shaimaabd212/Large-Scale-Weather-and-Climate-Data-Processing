# Large-Scale Weather and Climate Data Processing (BD05)

Distributed processing of NOAA **GHCN-Daily** weather observations on a 3-node
cluster — **1 master + 2 workers** running **HDFS** and **Apache Spark (standalone)** —
to produce climate summaries, extremes, seasonal trends and spatial aggregates.

```
             ┌──────────────── bd-master ────────────────┐
             │ HDFS NameNode  ·  Spark Master  ·  Driver │
             └───────────────┬───────────────────────────┘
                   ┌─────────┴─────────┐
        ┌──── bd-worker1 ────┐  ┌──── bd-worker2 ────┐
        │ HDFS DataNode      │  │ HDFS DataNode      │
        │ Spark Worker       │  │ Spark Worker       │
        └────────────────────┘  └────────────────────┘
```

## Pipeline

| Stage | Module | Input → Output | What it does |
|---|---|---|---|
| 1 ingest | `src/weather_pipeline/ingest.py` | raw csv.gz → `bronze/` Parquet | typed schema, element/country filter, parse station metadata |
| 2 clean | `clean.py` | bronze → `silver/daily` + `quality/` | units, missing values, QC flags, range checks, duplicates, TMIN>TMAX, pivot to one row per station-day |
| 3a aggregate | `aggregate.py` | silver → `gold/` | monthly, seasonal, annual summaries, baseline climatology, anomalies |
| 3b extremes | `extremes.py` | silver → `gold/` | ETCCDI-style indices (TXx, TNn, hot days, Rx1day, CDD…), record tables |
| 3c spatial | `spatial.py` | gold → `gold/` | grid-cell & area-weighted regional means, country summaries, OLS trends |
| 4 export | `export.py` | gold → `export/*.csv` | small CSVs for the notebook & report |

## Quick start

**On a laptop (development, 1 minute, no cluster needed)** — Linux, macOS or WSL2 on Windows, with Java 11/17 installed
```bash
pip install -r requirements.txt
python3 scripts/data/make_sample_data.py          # synthetic data in GHCN format
bash scripts/run_pipeline.sh --local --sample     # runs every stage with local[*]
jupyter notebook notebooks/01_climate_analysis.ipynb
```

**On the cluster** — full steps in [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md)
```bash
# every node
bash scripts/setup/01_install_node.sh master|worker1|worker2
bash scripts/setup/02_configure_node.sh
# master only
bash scripts/setup/03_setup_ssh.sh
bash scripts/setup/04_format_hdfs.sh
bash scripts/cluster/start_cluster.sh
bash scripts/data/download_ghcnd.sh
bash scripts/data/upload_to_hdfs.sh
bash scripts/run_pipeline.sh --count
bash scripts/benchmark.sh
```

## Repository layout

```
config/      cluster.env (machines, versions, resources) · pipeline.json (analysis settings)
             hadoop/ & spark/ config templates
data/        raw/ (downloads, git-ignored) · export/ (results) · sample/ (synthetic)
docs/        WORK_PLAN.md · SETUP_GUIDE.md · REPORT_OUTLINE.md
notebooks/   01_climate_analysis.ipynb — figures for the report
pipelines/   run_pipeline.py — the Spark driver (stages, timing, metrics)
scripts/     setup/ · cluster/ · data/ · run_pipeline.sh · benchmark.sh
src/         weather_pipeline/ — the PySpark code, one module per stage
logs/        run_*.json metrics and benchmark.csv
```

## Team

See [docs/WORK_PLAN.md](docs/WORK_PLAN.md) for who owns what.

## Data source & terms

NOAA NCEI Global Historical Climatology Network – Daily (GHCN-D), public domain (U.S. Government).
Cite: Menne, M.J. et al. (2012) *Global Historical Climatology Network – Daily (GHCN-Daily), Version 3*,
NOAA National Climatic Data Center. https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily
