#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Downloads NOAA GHCN-Daily yearly files + station metadata into data/raw/.
#   Usage: bash scripts/data/download_ghcnd.sh [YEAR_START] [YEAR_END]
# Defaults come from config/cluster.env. Safe to re-run (resumes downloads).
# Source: https://www.ncei.noaa.gov/pub/data/ghcn/daily/   (mirror: AWS noaa-ghcn-pds)
# ---------------------------------------------------------------------
source "$(dirname "$0")/../common.sh"

START="${1:-$YEAR_START}"
END="${2:-$YEAR_END}"
RAW="$REPO_ROOT/data/raw"
mkdir -p "$RAW/by_year" "$RAW/meta"

NCEI="https://www.ncei.noaa.gov/pub/data/ghcn/daily"
AWS="https://noaa-ghcn-pds.s3.amazonaws.com"

fetch() {  # fetch <dest> <url1> [url2]
  local dest="$1"; shift
  for url in "$@"; do
    if wget -q --show-progress -c -O "$dest.part" "$url"; then
      mv "$dest.part" "$dest"; return 0
    fi
    warn "Failed: $url — trying next source"
  done
  rm -f "$dest.part"
  return 1
}

info "Station metadata"
for f in ghcnd-stations.txt ghcnd-countries.txt ghcnd-inventory.txt; do
  [[ -s "$RAW/meta/$f" ]] && { ok "$f already present"; continue; }
  fetch "$RAW/meta/$f" "$NCEI/$f" "$AWS/$f" || fail "Could not download $f"
done
fetch "$RAW/meta/readme.txt" "$NCEI/readme.txt" || warn "readme.txt not downloaded (optional)"

info "Yearly observation files $START..$END"
for y in $(seq "$START" "$END"); do
  out="$RAW/by_year/$y.csv.gz"
  if [[ -s "$out" ]] && gzip -t "$out" 2>/dev/null; then ok "$y already downloaded"; continue; fi
  fetch "$out" "$NCEI/by_year/$y.csv.gz" "$AWS/csv.gz/$y.csv.gz" || fail "Could not download $y.csv.gz"
  gzip -t "$out" || fail "$out is corrupt — delete it and re-run"
  ok "$y done ($(du -h "$out" | cut -f1))"
done

du -sh "$RAW"
ok "Download complete. Next (on master): bash scripts/data/upload_to_hdfs.sh"
