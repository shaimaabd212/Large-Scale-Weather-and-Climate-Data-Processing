"""Stage 3a — CLIMATE SUMMARIES (gold): monthly, seasonal, annual, climatology, anomalies.

Completeness rules (config "completeness"):
  * a month is valid for temperature/precipitation if >= min_month_coverage of its days exist
  * a year's temperature anomaly needs >= min_valid_months_temp valid months
  * an annual precipitation total needs all min_valid_months_prcp months valid
  * a season needs all 3 months valid
  * a station's climatology (baseline) value needs >= min_baseline_years years
Using ANOMALIES (difference from each station's own baseline) rather than raw
temperatures when averaging across stations stops the result from changing just
because a hot or cold station starts/stops reporting.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Paths

STATION_META = ["station_id", "station_name", "country_code", "country_name",
                "latitude", "longitude", "elevation_m", "hemisphere"]


def monthly(daily: DataFrame, cfg: dict) -> DataFrame:
    cov = cfg["completeness"]["min_month_coverage"]
    m = daily.groupBy("station_id", "year", "month").agg(
        F.count("tmean_c").alias("n_days_temp"),
        F.avg("tmean_c").alias("tmean_c"),
        F.avg("tmax_c").alias("tmax_mean_c"),
        F.avg("tmin_c").alias("tmin_mean_c"),
        F.max("tmax_c").alias("tmax_max_c"),
        F.min("tmin_c").alias("tmin_min_c"),
        F.count("prcp_mm").alias("n_days_prcp"),
        F.sum("prcp_mm").alias("prcp_total_mm"),
        F.max("prcp_mm").alias("prcp_max_mm"),
        F.sum("snow_mm").alias("snow_total_mm"),
    )
    dim = F.dayofmonth(F.last_day(F.make_date(F.col("year"), F.col("month"), F.lit(1))))
    return (m.withColumn("days_in_month", dim)
             .withColumn("temp_valid", (F.col("n_days_temp") / F.col("days_in_month")) >= cov)
             .withColumn("prcp_valid", (F.col("n_days_prcp") / F.col("days_in_month")) >= cov))


def _in_baseline(cfg: dict):
    y0, y1 = cfg["baseline_years"]
    return F.col("year").between(y0, y1)


def monthly_climatology(m: DataFrame, cfg: dict) -> DataFrame:
    """Per station and calendar month: baseline-period mean temperature and precipitation."""
    min_years = cfg["completeness"]["min_baseline_years"]
    clim = (m.filter(_in_baseline(cfg))
             .groupBy("station_id", "month").agg(
                 F.avg(F.when(F.col("temp_valid"), F.col("tmean_c"))).alias("clim_tmean_c"),
                 F.count(F.when(F.col("temp_valid"), 1)).alias("clim_years_temp"),
                 F.avg(F.when(F.col("prcp_valid"), F.col("prcp_total_mm"))).alias("clim_prcp_mm"),
                 F.count(F.when(F.col("prcp_valid"), 1)).alias("clim_years_prcp")))
    return (clim.withColumn("clim_tmean_c", F.when(F.col("clim_years_temp") >= min_years, F.col("clim_tmean_c")))
                .withColumn("clim_prcp_mm", F.when(F.col("clim_years_prcp") >= min_years, F.col("clim_prcp_mm"))))


def monthly_anomalies(m: DataFrame, clim: DataFrame) -> DataFrame:
    a = m.join(clim.select("station_id", "month", "clim_tmean_c", "clim_prcp_mm"),
               ["station_id", "month"], "left")
    return (a.withColumn("tmean_anom_c",
                         F.when(F.col("temp_valid"), F.col("tmean_c") - F.col("clim_tmean_c")))
             .withColumn("prcp_anom_pct",
                         F.when(F.col("prcp_valid") & (F.col("clim_prcp_mm") > 0),
                                100 * (F.col("prcp_total_mm") - F.col("clim_prcp_mm")) / F.col("clim_prcp_mm"))))


def annual(m_anom: DataFrame, cfg: dict) -> DataFrame:
    c = cfg["completeness"]
    a = m_anom.groupBy("station_id", "year").agg(
        F.count(F.when(F.col("temp_valid"), 1)).alias("n_valid_months_temp"),
        F.count(F.when(F.col("prcp_valid"), 1)).alias("n_valid_months_prcp"),
        F.avg(F.when(F.col("temp_valid"), F.col("tmean_c"))).alias("tmean_c"),
        F.avg("tmean_anom_c").alias("tmean_anom_c"),
        F.count("tmean_anom_c").alias("n_months_anom"),
        F.sum(F.when(F.col("prcp_valid"), F.col("prcp_total_mm"))).alias("prcp_total_mm"),
    )
    a = (a.withColumn("tmean_c", F.when(F.col("n_valid_months_temp") >= c["min_valid_months_temp"], F.col("tmean_c")))
          .withColumn("tmean_anom_c", F.when(F.col("n_months_anom") >= c["min_valid_months_temp"], F.col("tmean_anom_c")))
          .withColumn("prcp_total_mm", F.when(F.col("n_valid_months_prcp") >= c["min_valid_months_prcp"], F.col("prcp_total_mm"))))

    # annual precipitation anomaly against the station's baseline-mean annual total
    y0, y1 = cfg["baseline_years"]
    base = (a.filter(F.col("year").between(y0, y1) & F.col("prcp_total_mm").isNotNull())
             .groupBy("station_id").agg(F.avg("prcp_total_mm").alias("clim_prcp_annual_mm"),
                                        F.count("*").alias("n_base")))
    base = base.filter(F.col("n_base") >= c["min_baseline_years"]).drop("n_base")
    a = a.join(base, "station_id", "left")
    return a.withColumn("prcp_anom_pct",
                        F.when(F.col("clim_prcp_annual_mm") > 0,
                               100 * (F.col("prcp_total_mm") - F.col("clim_prcp_annual_mm")) / F.col("clim_prcp_annual_mm")))


def seasonal(m_anom: DataFrame, cfg: dict) -> DataFrame:
    """Meteorological seasons. December counts toward the NEXT year's DJF.
    Labels are calendar-based: DJF is winter in the north but summer in the south
    (use the hemisphere column when interpreting)."""
    season = (F.when(F.col("month").isin(12, 1, 2), "DJF")
               .when(F.col("month").isin(3, 4, 5), "MAM")
               .when(F.col("month").isin(6, 7, 8), "JJA")
               .otherwise("SON"))
    season_year = F.when(F.col("month") == 12, F.col("year") + 1).otherwise(F.col("year"))
    s = (m_anom.withColumn("season", season).withColumn("season_year", season_year)
               .groupBy("station_id", "season_year", "season").agg(
                   F.count(F.when(F.col("temp_valid"), 1)).alias("n_valid_temp"),
                   F.count(F.when(F.col("prcp_valid"), 1)).alias("n_valid_prcp"),
                   F.avg(F.when(F.col("temp_valid"), F.col("tmean_c"))).alias("tmean_c"),
                   F.avg("tmean_anom_c").alias("tmean_anom_c"),
                   F.count("tmean_anom_c").alias("n_anom"),
                   F.sum(F.when(F.col("prcp_valid"), F.col("prcp_total_mm"))).alias("prcp_total_mm")))
    return (s.withColumn("tmean_c", F.when(F.col("n_valid_temp") == 3, F.col("tmean_c")))
             .withColumn("tmean_anom_c", F.when(F.col("n_anom") == 3, F.col("tmean_anom_c")))
             .withColumn("prcp_total_mm", F.when(F.col("n_valid_prcp") == 3, F.col("prcp_total_mm")))
             .drop("n_anom"))


def run(spark: SparkSession, paths: Paths, cfg: dict, metrics: dict) -> None:
    daily = spark.read.parquet(paths.silver_daily)
    stations = daily.select(*STATION_META).dropDuplicates(["station_id"])
    stations.write.mode("overwrite").parquet(paths.gold("stations_used"))
    stations = spark.read.parquet(paths.gold("stations_used"))

    m = monthly(daily, cfg)
    m.write.mode("overwrite").partitionBy("year").parquet(paths.gold("_monthly_tmp"))
    m = spark.read.parquet(paths.gold("_monthly_tmp"))

    clim = monthly_climatology(m, cfg)
    clim.join(F.broadcast(stations), "station_id", "left") \
        .write.mode("overwrite").parquet(paths.gold("monthly_climatology"))

    m_anom = monthly_anomalies(m, spark.read.parquet(paths.gold("monthly_climatology")))
    m_anom.join(F.broadcast(stations), "station_id", "left") \
          .write.mode("overwrite").partitionBy("year").parquet(paths.gold("monthly_station"))
    m_anom = spark.read.parquet(paths.gold("monthly_station"))

    annual(m_anom, cfg).join(F.broadcast(stations), "station_id", "left") \
        .write.mode("overwrite").parquet(paths.gold("annual_station"))

    seasonal(m_anom, cfg).join(F.broadcast(stations), "station_id", "left") \
        .write.mode("overwrite").parquet(paths.gold("seasonal_station"))

    if metrics.get("_count"):
        metrics["monthly_rows"] = m_anom.count()
        metrics["annual_rows"] = spark.read.parquet(paths.gold("annual_station")).count()
