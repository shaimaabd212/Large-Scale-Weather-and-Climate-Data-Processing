"""Stage 3b — EXTREMES (gold): ETCCDI-style climate-extreme indices and record tables.

Indices per station-year (thresholds in config "thresholds"):
  TXx / TNn          hottest day-max, coldest night-min
  frost_days         TMIN < 0 °C           icing_days   TMAX < 0 °C
  summer_days        TMAX > 25 °C          hot_days     TMAX >= 35 °C
  tropical_nights    TMIN > 20 °C
  Rx1day             wettest day           PRCPTOT      total on wet days (>= 1 mm)
  R10mm / R20mm      heavy / very heavy precipitation days
  SDII               PRCPTOT / wet days    CDD / CWD    longest dry / wet spell (days)
Day-count indices are only comparable between years when coverage is high, so
coverage_ok_* flags are included — filter on them before comparing years.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import Paths


def _count_if(cond):
    return F.sum(F.when(cond, 1).otherwise(0))


def station_year_indices(daily: DataFrame, cfg: dict) -> DataFrame:
    t = cfg["thresholds"]
    tx, tn, pr = F.col("tmax_c"), F.col("tmin_c"), F.col("prcp_mm")
    idx = daily.groupBy("station_id", "year").agg(
        F.count(tx).alias("n_days_tmax"),
        F.count(tn).alias("n_days_tmin"),
        F.count(pr).alias("n_days_prcp"),
        F.max(tx).alias("txx_c"),
        F.min(tn).alias("tnn_c"),
        F.min(tx).alias("txn_c"),
        F.max(tn).alias("tnx_c"),
        _count_if(tn < 0).alias("frost_days"),
        _count_if(tx < 0).alias("icing_days"),
        _count_if(tx > t["summer_day_c"]).alias("summer_days"),
        _count_if(tx >= t["hot_day_c"]).alias("hot_days"),
        _count_if(tn > t["tropical_night_c"]).alias("tropical_nights"),
        F.max(pr).alias("rx1day_mm"),
        _count_if(pr >= t["wet_day_mm"]).alias("wet_days"),
        F.sum(F.when(pr >= t["wet_day_mm"], pr)).alias("prcptot_mm"),
        _count_if(pr >= t["heavy_prcp_mm"]).alias("r10mm_days"),
        _count_if(pr >= t["very_heavy_prcp_mm"]).alias("r20mm_days"),
    )
    min_days = 0.8 * 365
    return (idx.withColumn("sdii_mm_per_day", F.col("prcptot_mm") / F.col("wet_days"))
               .withColumn("coverage_ok_temp", (F.col("n_days_tmax") >= min_days) & (F.col("n_days_tmin") >= min_days))
               .withColumn("coverage_ok_prcp", F.col("n_days_prcp") >= min_days))


def longest_spells(daily: DataFrame, cfg: dict) -> DataFrame:
    """CDD/CWD with the 'gaps and islands' trick: for consecutive calendar days,
    (day number - row number) is constant, so it labels each run. A missing day
    breaks a run (conservative). Spells are counted within a calendar year."""
    wet_mm = cfg["thresholds"]["wet_day_mm"]
    p = (daily.filter(F.col("prcp_mm").isNotNull())
              .select("station_id", "year", "date",
                      (F.col("prcp_mm") >= wet_mm).alias("is_wet")))
    w = Window.partitionBy("station_id", "year", "is_wet").orderBy("date")
    day_no = F.datediff(F.col("date"), F.to_date(F.lit("1970-01-01")))
    runs = (p.withColumn("run_id", day_no - F.row_number().over(w))
             .groupBy("station_id", "year", "is_wet", "run_id").count())
    return runs.groupBy("station_id", "year").agg(
        F.max(F.when(~F.col("is_wet"), F.col("count"))).alias("cdd_days"),
        F.max(F.when(F.col("is_wet"), F.col("count"))).alias("cwd_days"))


def top_n(daily: DataFrame, col: str, n: int, ascending: bool) -> DataFrame:
    order = F.col(col).asc() if ascending else F.col(col).desc()
    return (daily.filter(F.col(col).isNotNull())
                 .orderBy(order).limit(n)
                 .select("station_id", "station_name", "country_name", "latitude", "longitude",
                         "date", col))


def country_records(idx: DataFrame) -> DataFrame:
    """Per country: hottest TXx, coldest TNn, wettest Rx1day, with station and year."""
    out = None
    for metric, ascending, label in [("txx_c", False, "hottest_day_c"),
                                     ("tnn_c", True, "coldest_night_c"),
                                     ("rx1day_mm", False, "wettest_day_mm")]:
        order = F.col(metric).asc() if ascending else F.col(metric).desc()
        w = Window.partitionBy("country_code").orderBy(order)
        r = (idx.filter(F.col(metric).isNotNull())
                .withColumn("rk", F.row_number().over(w)).filter("rk = 1")
                .select("country_code", "country_name", F.lit(label).alias("record"),
                        F.col(metric).alias("value"), "station_id", "station_name", "year"))
        out = r if out is None else out.unionByName(r)
    return out


def run(spark: SparkSession, paths: Paths, cfg: dict, metrics: dict) -> None:
    daily = spark.read.parquet(paths.silver_daily)
    stations = spark.read.parquet(paths.gold("stations_used"))

    idx = station_year_indices(daily, cfg).join(longest_spells(daily, cfg), ["station_id", "year"], "left")
    (idx.join(F.broadcast(stations), "station_id", "left")
        .write.mode("overwrite").parquet(paths.gold("extreme_indices")))
    idx = spark.read.parquet(paths.gold("extreme_indices"))

    n = cfg["top_n_extremes"]
    top_n(daily, "tmax_c", n, ascending=False).write.mode("overwrite").parquet(paths.gold("top_hottest_days"))
    top_n(daily, "tmin_c", n, ascending=True).write.mode("overwrite").parquet(paths.gold("top_coldest_days"))
    top_n(daily, "prcp_mm", n, ascending=False).write.mode("overwrite").parquet(paths.gold("top_wettest_days"))
    country_records(idx).write.mode("overwrite").parquet(paths.gold("country_records"))

    if metrics.get("_count"):
        metrics["extreme_index_rows"] = idx.count()
