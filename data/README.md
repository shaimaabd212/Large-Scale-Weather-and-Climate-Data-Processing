# data/

Nothing in here is committed except this file (see `.gitignore`).

| Folder | Filled by | Contents |
|---|---|---|
| `raw/by_year/` | `scripts/data/download_ghcnd.sh` | `YYYY.csv.gz` — one GHCN-Daily file per year |
| `raw/meta/` | same | `ghcnd-stations.txt`, `ghcnd-countries.txt`, `ghcnd-inventory.txt`, `readme.txt` |
| `sample/` | `scripts/data/make_sample_data.py` | small synthetic dataset for laptop testing |
| `export/` | `scripts/run_pipeline.sh` | CSV summaries copied from HDFS for the notebook |

On the cluster the working data (bronze / silver / gold / quality / export) lives in HDFS under `/weather/`.

## Raw format (by_year CSV, no header)

| # | Field | Notes |
|---|---|---|
| 1 | ID | 11 chars; first 2 = FIPS country code (LY = Libya) |
| 2 | DATE | YYYYMMDD |
| 3 | ELEMENT | TMAX, TMIN, TAVG (tenths °C) · PRCP (tenths mm) · SNOW, SNWD (mm) · … |
| 4 | DATA VALUE | integer; -9999 = missing |
| 5 | M-FLAG | measurement flag |
| 6 | Q-FLAG | quality flag — **blank = passed** all NOAA QC checks |
| 7 | S-FLAG | source flag |
| 8 | OBS-TIME | HHMM, often blank |

Authoritative definitions: https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt
