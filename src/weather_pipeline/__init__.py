"""Large-Scale Weather and Climate Data Processing — PySpark pipeline (GHCN-Daily).

Medallion layout under a base path (HDFS or local):
    raw/      original NOAA files (csv.gz + fixed-width metadata)
    bronze/   typed, unfiltered copy in Parquet           (ingest.py)
    silver/   cleaned, harmonised daily table + QC tables  (clean.py)
    gold/     climate summaries, extremes, spatial, trends (aggregate.py, extremes.py, spatial.py)
    export/   small CSVs for the notebook / report         (export.py)
"""
__version__ = "1.0.0"
