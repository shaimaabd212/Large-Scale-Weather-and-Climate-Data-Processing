"""Stage 4 — EXPORT: small gold tables -> single CSV files for the notebook and report.
Big tables (daily, monthly) stay in Parquet; only summaries leave the cluster."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Paths


def _csv(df: DataFrame, path: str) -> None:
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)


def run(spark: SparkSession, paths: Paths, cfg: dict, metrics: dict) -> None:
    g = lambda name: spark.read.parquet(paths.gold(name))          # noqa: E731
    q = lambda name: spark.read.parquet(paths.quality(name))       # noqa: E731
    focus = cfg.get("focus_countries", [])

    tables = {
        "regional_annual": g("regional_annual").orderBy("region", "year"),
        "country_annual": g("country_annual").orderBy("country_code", "year"),
        "country_seasonal": g("country_seasonal").orderBy("country_code", "season_year", "season"),
        "grid_annual": g("grid_annual"),
        "station_trends": g("station_trends"),
        "country_records": g("country_records").orderBy("country_code", "record"),
        "top_hottest_days": g("top_hottest_days"),
        "top_coldest_days": g("top_coldest_days"),
        "top_wettest_days": g("top_wettest_days"),
        "country_monthly_climatology": (
            g("monthly_climatology").groupBy("country_code", "country_name", "month").agg(
                F.avg("clim_tmean_c").alias("clim_tmean_c"),
                F.avg("clim_prcp_mm").alias("clim_prcp_mm"),
                F.count("clim_tmean_c").alias("n_stations"))
            .orderBy("country_code", "month")),
        "focus_annual_station": (
            g("annual_station").join(
                g("extreme_indices").drop("station_name", "country_code", "country_name", "latitude",
                                          "longitude", "elevation_m", "hemisphere"),
                ["station_id", "year"], "left")
            .filter(F.col("country_code").isin(focus))),
        "focus_station_season_trends": g("station_season_trends").filter(F.col("country_code").isin(focus)),
        "quality_record_status": q("record_status").orderBy("year", "element", "status"),
        "quality_qflag_breakdown": q("qflag_breakdown").orderBy(F.desc("count")),
        "quality_coverage_by_year": (
            q("station_year_coverage").groupBy("year").agg(
                F.countDistinct("station_id").alias("stations"),
                F.avg("coverage_tmean").alias("avg_coverage_tmean"),
                F.avg("coverage_prcp").alias("avg_coverage_prcp"),
                F.sum(F.when(F.col("coverage_tmean") >= 0.8, 1).otherwise(0)).alias("stations_tmean_ge_80pct"),
                F.sum("duplicates").alias("duplicates"),
                F.sum("tmin_gt_tmax").alias("tmin_gt_tmax_days"))
            .orderBy("year")),
    }
    for name, df in tables.items():
        _csv(df, paths.export(name))
    metrics["exported_tables"] = list(tables)
