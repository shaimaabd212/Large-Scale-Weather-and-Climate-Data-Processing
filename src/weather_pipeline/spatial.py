"""Stage 3c — SPATIAL AGGREGATES & TRENDS (gold).

Spatial averaging is done in two steps so dense station networks (e.g. the USA)
do not dominate a global mean:
  1. average station anomalies inside each lat/lon grid cell (config grid_size_deg)
  2. average the cells, weighted by cos(latitude) (cells shrink towards the poles)

Trends are ordinary least-squares slopes, computed in Spark with aggregate functions:
  slope = cov(year, y) / var(year)      (reported per decade)
A trend describes change over the period. It does NOT by itself explain the cause
(see docs/REPORT_OUTLINE.md, "descriptive vs causal").
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Paths


def _cell(col: str, size: float):
    return F.floor(F.col(col) / size) * size + size / 2


def grid_annual(annual: DataFrame, cfg: dict) -> DataFrame:
    g = cfg["grid_size_deg"]
    return (annual.filter(F.col("tmean_anom_c").isNotNull())
                  .withColumn("cell_lat", _cell("latitude", g))
                  .withColumn("cell_lon", _cell("longitude", g))
                  .groupBy("year", "cell_lat", "cell_lon").agg(
                      F.avg("tmean_anom_c").alias("tmean_anom_c"),
                      F.avg("prcp_anom_pct").alias("prcp_anom_pct"),
                      F.countDistinct("station_id").alias("n_stations")))


def regional_annual(grid: DataFrame) -> DataFrame:
    w = F.cos(F.radians(F.col("cell_lat")))
    lo = (F.floor(F.col("cell_lat") / 30) * 30).cast("int")
    band = F.format_string("Lat band %d to %d", lo, lo + 30)
    regions = [("Global", F.lit(True)), ("Northern Hemisphere", F.col("cell_lat") >= 0),
               ("Southern Hemisphere", F.col("cell_lat") < 0)]
    out = None
    for name, cond in regions:
        r = (grid.filter(cond).withColumn("w", w)
                 .groupBy("year").agg(
                     (F.sum(F.col("tmean_anom_c") * F.col("w")) / F.sum("w")).alias("tmean_anom_c"),
                     F.count("*").alias("n_cells"), F.sum("n_stations").alias("n_stations"))
                 .withColumn("region", F.lit(name)))
        out = r if out is None else out.unionByName(r)
    bands = (grid.withColumn("w", w).withColumn("region", band)
                 .groupBy("region", "year").agg(
                     (F.sum(F.col("tmean_anom_c") * F.col("w")) / F.sum("w")).alias("tmean_anom_c"),
                     F.count("*").alias("n_cells"), F.sum("n_stations").alias("n_stations")))
    return out.unionByName(bands)


def country_annual(annual: DataFrame, idx: DataFrame) -> DataFrame:
    a = annual.groupBy("country_code", "country_name", "year").agg(
        F.avg("tmean_anom_c").alias("tmean_anom_c"),
        F.avg("tmean_c").alias("tmean_c"),
        F.avg("prcp_anom_pct").alias("prcp_anom_pct"),
        F.count("tmean_anom_c").alias("n_stations_temp"),
        F.count("prcp_anom_pct").alias("n_stations_prcp"))
    i = (idx.filter("coverage_ok_temp")
            .groupBy("country_code", "year").agg(
                F.avg("hot_days").alias("avg_hot_days"),
                F.avg("summer_days").alias("avg_summer_days"),
                F.avg("tropical_nights").alias("avg_tropical_nights"),
                F.avg("frost_days").alias("avg_frost_days"),
                F.max("txx_c").alias("max_txx_c"),
                F.min("tnn_c").alias("min_tnn_c")))
    p = (idx.filter("coverage_ok_prcp")
            .groupBy("country_code", "year").agg(
                F.avg("rx1day_mm").alias("avg_rx1day_mm"),
                F.avg("cdd_days").alias("avg_cdd_days"),
                F.avg("r20mm_days").alias("avg_r20mm_days")))
    return a.join(i, ["country_code", "year"], "left").join(p, ["country_code", "year"], "left")


def country_seasonal(seasonal: DataFrame) -> DataFrame:
    return seasonal.groupBy("country_code", "country_name", "season_year", "season").agg(
        F.avg("tmean_anom_c").alias("tmean_anom_c"),
        F.avg("tmean_c").alias("tmean_c"),
        F.avg("prcp_total_mm").alias("prcp_total_mm"),
        F.count("tmean_anom_c").alias("n_stations"))


def ols_trend(df: DataFrame, keys: list, x: str, y: str, min_n: int, per: float = 10.0) -> DataFrame:
    d = df.filter(F.col(y).isNotNull())
    t = d.groupBy(*keys).agg(
        F.count(y).alias("n_years"),
        F.min(x).alias("first_year"), F.max(x).alias("last_year"),
        (F.covar_samp(x, y) / F.var_samp(x) * per).alias(f"{y}_trend_per_decade"),
        F.corr(x, y).alias("r"))
    return (t.filter(F.col("n_years") >= min_n)
             .withColumn("r2", F.col("r") ** 2).drop("r"))


def run(spark: SparkSession, paths: Paths, cfg: dict, metrics: dict) -> None:
    annual = spark.read.parquet(paths.gold("annual_station"))
    seasonal = spark.read.parquet(paths.gold("seasonal_station"))
    idx = spark.read.parquet(paths.gold("extreme_indices"))
    stations = spark.read.parquet(paths.gold("stations_used"))
    min_n = cfg["completeness"]["min_years_for_trend"]

    grid = grid_annual(annual, cfg)
    grid.write.mode("overwrite").parquet(paths.gold("grid_annual"))
    grid = spark.read.parquet(paths.gold("grid_annual"))
    regional_annual(grid).write.mode("overwrite").parquet(paths.gold("regional_annual"))

    country_annual(annual, idx).write.mode("overwrite").parquet(paths.gold("country_annual"))
    country_seasonal(seasonal).write.mode("overwrite").parquet(paths.gold("country_seasonal"))

    # per-station trends: temperature, precipitation, hot days
    t_temp = ols_trend(annual, ["station_id"], "year", "tmean_anom_c", min_n)
    t_prcp = ols_trend(annual, ["station_id"], "year", "prcp_total_mm", min_n).select(
        "station_id", "prcp_total_mm_trend_per_decade")
    t_hot = ols_trend(idx.filter("coverage_ok_temp"), ["station_id"], "year", "hot_days", min_n).select(
        "station_id", "hot_days_trend_per_decade")
    (t_temp.join(t_prcp, "station_id", "left").join(t_hot, "station_id", "left")
           .join(F.broadcast(stations), "station_id", "left")
           .write.mode("overwrite").parquet(paths.gold("station_trends")))

    # seasonal trends per station
    s = seasonal.withColumnRenamed("season_year", "year")
    (ols_trend(s, ["station_id", "season"], "year", "tmean_anom_c", min_n)
        .join(F.broadcast(stations), "station_id", "left")
        .write.mode("overwrite").parquet(paths.gold("station_season_trends")))

    if metrics.get("_count"):
        metrics["grid_cells"] = grid.select("cell_lat", "cell_lon").distinct().count()
        metrics["stations_with_trend"] = spark.read.parquet(paths.gold("station_trends")).count()
