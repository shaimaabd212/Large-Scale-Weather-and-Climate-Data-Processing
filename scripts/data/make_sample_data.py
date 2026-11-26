#!/usr/bin/env python3
"""
Generate a SMALL synthetic dataset in the exact GHCN-Daily formats so the
pipeline can be developed and tested on a laptop in seconds (no download).

    python3 scripts/data/make_sample_data.py            # -> data/sample/raw/...
    bash scripts/run_pipeline.sh --local --sample

The data contains deliberate problems so the cleaning code has something to do:
missing values (-9999), failed quality flags, duplicate records, impossible
values, TMIN > TMAX days, and missing days.

Uses only the Python standard library.
"""
import argparse
import gzip
import math
import random
from datetime import date, timedelta
from pathlib import Path

# (id, lat, lon, elev, name) — fictional stations at realistic locations
STATIONS = [
    ("LY000062010", 32.90, 13.28, 81.0, "TRIPOLI SAMPLE"),
    ("LY000062053", 32.10, 20.27, 129.0, "BENGHAZI SAMPLE"),
    ("LY000062124", 27.03, 14.43, 432.0, "SEBHA SAMPLE"),
    ("EG000062366", 30.13, 31.40, 74.0, "CAIRO SAMPLE"),
    ("EG000062414", 24.03, 32.88, 194.0, "ASWAN SAMPLE"),
    ("TS000060715", 36.83, 10.23, 3.0, "TUNIS SAMPLE"),
    ("IT000016239", 41.80, 12.23, 3.0, "ROMA SAMPLE"),
    ("IT000016080", 45.43, 9.28, 103.0, "MILANO SAMPLE"),
    ("SP000003195", 40.41, -3.68, 667.0, "MADRID SAMPLE"),
    ("UK000003772", 51.48, -0.45, 25.0, "LONDON SAMPLE"),
    ("RSM00027612", 55.83, 37.62, 147.0, "MOSCOW SAMPLE"),
    ("CA006158355", 43.67, -79.40, 113.0, "TORONTO SAMPLE"),
    ("USW00094728", 40.78, -73.97, 39.6, "NEW YORK SAMPLE"),
    ("USW00023183", 33.43, -112.01, 337.0, "PHOENIX SAMPLE"),
    ("BR000083743", -22.90, -43.17, 5.0, "RIO SAMPLE"),
    ("ASN00066062", -33.86, 151.21, 39.0, "SYDNEY SAMPLE"),
    ("SF000068816", -33.97, 18.60, 46.0, "CAPE TOWN SAMPLE"),
    ("IN022021000", 28.58, 77.20, 216.0, "NEW DELHI SAMPLE"),
    ("JA000047662", 35.68, 139.77, 36.0, "TOKYO SAMPLE"),
    ("NO000001492", 59.94, 10.72, 94.0, "OSLO SAMPLE"),
]
COUNTRIES = {
    "LY": "Libya", "EG": "Egypt", "TS": "Tunisia", "IT": "Italy", "SP": "Spain",
    "UK": "United Kingdom", "RS": "Russia", "CA": "Canada", "US": "United States",
    "BR": "Brazil", "AS": "Australia", "SF": "South Africa", "IN": "India",
    "JA": "Japan", "NO": "Norway",
}


def station_line(sid, lat, lon, elev, name):
    # Fixed-width layout from the GHCN-D readme (columns 1-85)
    return f"{sid:<11} {lat:8.4f} {lon:9.4f} {elev:6.1f} {'':<2} {name:<30} {'GSN':<3} {'':<3} {'':<5}"


def daily_values(lat, d, year0, rng):
    """Seasonal temperature cycle + warming trend + noise; random rain."""
    doy = d.timetuple().tm_yday
    phase = 200 if lat >= 0 else 20                       # warmest day of year
    mean_annual = 28 - 0.45 * abs(lat)
    amplitude = 3 + 0.25 * abs(lat)
    trend = 0.03 * (d.year - year0)                        # +0.3 °C / decade
    tmean = mean_annual + amplitude * math.cos(2 * math.pi * (doy - phase) / 365.25) + trend
    tmean += rng.gauss(0, 2.0)
    dtr = max(2.0, rng.gauss(10, 2))                       # diurnal range
    tmax, tmin = tmean + dtr / 2, tmean - dtr / 2
    prcp = rng.expovariate(1 / 8.0) if rng.random() < 0.25 else 0.0
    snow = prcp * 10 if tmax < 0 and prcp > 0 else 0.0
    return tmax, tmin, prcp, snow


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/sample/raw")
    ap.add_argument("--start", type=int, default=2014)
    ap.add_argument("--end", type=int, default=2023)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)
    (out / "by_year").mkdir(parents=True, exist_ok=True)
    (out / "meta").mkdir(parents=True, exist_ok=True)

    (out / "meta" / "ghcnd-stations.txt").write_text(
        "\n".join(station_line(*s) for s in STATIONS) + "\n")
    (out / "meta" / "ghcnd-countries.txt").write_text(
        "\n".join(f"{k} {v}" for k, v in COUNTRIES.items()) + "\n")
    (out / "meta" / "ghcnd-inventory.txt").write_text("")

    stats = {"rows": 0, "missing": 0, "qflag": 0, "dupes": 0, "outliers": 0, "swapped": 0}
    for year in range(args.start, args.end + 1):
        rows = []
        d = date(year, 1, 1)
        while d.year == year:
            for sid, lat, lon, elev, name in STATIONS:
                if rng.random() < 0.03:                    # whole day missing
                    d_skip = True
                else:
                    d_skip = False
                if d_skip:
                    continue
                tmax, tmin, prcp, snow = daily_values(lat, d, args.start, rng)
                if rng.random() < 0.002:                   # TMIN > TMAX error
                    tmax, tmin = tmin, tmax
                    stats["swapped"] += 1
                ds = d.strftime("%Y%m%d")
                recs = [("TMAX", round(tmax * 10)), ("TMIN", round(tmin * 10)),
                        ("PRCP", round(prcp * 10))]
                if snow > 0:
                    recs.append(("SNOW", round(snow)))
                for el, val in recs:
                    q = ""
                    r = rng.random()
                    if r < 0.003:
                        val = -9999; stats["missing"] += 1
                    elif r < 0.006:
                        q = rng.choice("DGIKLMNORSTWXZ"); stats["qflag"] += 1
                    elif r < 0.007 and el in ("TMAX", "TMIN"):
                        val = 999 * (1 if el == "TMAX" else -1) * 10; stats["outliers"] += 1
                    rows.append(f"{sid},{ds},{el},{val},,{q},S,0700")
                    if rng.random() < 0.001:
                        rows.append(f"{sid},{ds},{el},{val},,{q},S,0700"); stats["dupes"] += 1
            d += timedelta(days=1)
        rng.shuffle(rows)                                  # real files are not sorted
        with gzip.open(out / "by_year" / f"{year}.csv.gz", "wt") as fh:
            fh.write("\n".join(rows) + "\n")
        stats["rows"] += len(rows)
        print(f"  {year}: {len(rows):,} rows")

    print(f"Sample data written to {out}/  ->  {stats}")


if __name__ == "__main__":
    main()
