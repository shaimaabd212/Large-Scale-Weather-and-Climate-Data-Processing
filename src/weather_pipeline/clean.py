"""Stage 2 — CLEAN & HARMONISE: bronze observations -> one row per station-day (silver).

Harmonisation decisions (explain these in the report):
  * Units: GHCN stores TMAX/TMIN/TAVG and PRCP in TENTHS (°C, mm). We convert to °C and mm.
           SNOW and SNWD are already in mm.
  * Missing values: -9999 (or unparsable) -> dropped.
  * Quality flags: any non-blank Q-FLAG means the value FAILED a NOAA QC check -> dropped.
  * Physical range checks: values outside config "valid_ranges" -> dropped.
  * Duplicates: several records for the same station/day/element -> averaged, and counted.
  * Internal consistency: TMIN > TMAX on the same day -> both set to null, and counted.
  * Daily mean temperature: tmean_c = (TMAX + TMIN) / 2 when both exist, else the reported
    TAVG. Most stations do not report TAVG, and stations compute it differently, so the
    (TMAX+TMIN)/2 definition gives a comparable series across stations.
  * Every row is classified (ok / missing / qc_failed / out_of_range) and the counts are
    written to quality/ so the cleaning is auditable.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Paths

TENTHS = ["TMAX", "TMIN", "TAVG", "PRCP"]
COLUMN_NAMES = {"TMAX": "tmax_c", "TMIN": "tmin_c", "TAVG": "tavg_c",
                "PRCP": "prcp_mm", "SNOW": "snow_mm", "SNWD": "snwd_mm"}


def classify(obs: DataFrame, cfg: dict) -> DataFrame:
    value = (F.when(F.col("element").isin(TENTHS), F.col("value_raw") / 10.0)
              .otherwise(F.col("value_raw").cast("double")))
    obs = obs.withColumn("value", value)

    ranges = cfg["valid_ranges"]
    in_range = ~F.col("element").isin(list(ranges))            # no rule -> accepted
    for el, (lo, hi) in ranges.items():
        in_range = in_range | ((F.col("element") == el) & F.col("value").between(lo, hi))

    status = (F.when(F.col("value_raw").isNull() | (F.col("value_raw") == -9999), "missing")
               .when(F.col("q_flag") != "", "qc_failed")
               .when(~in_range, "out_of_range")
               .otherwise("ok"))
    return obs.withColumn("status", status)


def to_daily(good: DataFrame, cfg: dict) -> DataFrame:
    elements = cfg["elements"]
    wide = (good.groupBy("station_id", "date", "year")
                .pivot("element", elements)
                .agg(F.avg("value").alias("v"), F.count("value").alias("n")))

    dupes = F.lit(0)
    for el in elements:
        wide = wide.withColumnRenamed(f"{el}_v", COLUMN_NAMES.get(el, el.lower()))
        dupes = dupes + F.greatest(F.coalesce(F.col(f"{el}_n"), F.lit(0)) - 1, F.lit(0))
    wide = wide.withColumn("n_duplicates", dupes.cast("int"))
    wide = wide.drop(*[f"{el}_n" for el in elements])

    for col in COLUMN_NAMES.values():                    # columns the later stages expect
        if col not in wide.columns:
            wide = wide.withColumn(col, F.lit(None).cast("double"))

    # Flag first, then null BOTH values using the flag (not the comparison, which
    # would change after the first column is nulled).
    bad_pair = F.coalesce(F.col("tmin_c") > F.col("tmax_c"), F.lit(False))
    wide = wide.withColumn("flag_tmin_gt_tmax", bad_pair.cast("int"))
    is_bad = F.col("flag_tmin_gt_tmax") == 1
    wide = (wide.withColumn("tmax_c", F.when(is_bad, F.lit(None)).otherwise(F.col("tmax_c")))
                .withColumn("tmin_c", F.when(is_bad, F.lit(None)).otherwise(F.col("tmin_c"))))

    both = F.col("tmax_c").isNotNull() & F.col("tmin_c").isNotNull()
    wide = (wide.withColumn("tmean_c", F.when(both, (F.col("tmax_c") + F.col("tmin_c")) / 2)
                                         .otherwise(F.col("tavg_c")))
                .withColumn("tmean_source", F.when(both, "tmax_tmin")
                                              .when(F.col("tavg_c").isNotNull(), "tavg")))
    return (wide.withColumn("month", F.month("date"))
                .withColumn("day_of_year", F.dayofyear("date")))


def run(spark: SparkSession, paths: Paths, cfg: dict, metrics: dict) -> None:
    obs = classify(spark.read.parquet(paths.bronze_obs), cfg)

    # --- audit tables ------------------------------------------------
    (obs.groupBy("year", "element", "status").count()
        .coalesce(1).write.mode("overwrite").parquet(paths.quality("record_status")))
    (obs.filter(F.col("status") == "qc_failed")
        .groupBy("element", "q_flag").count()
        .coalesce(1).write.mode("overwrite").parquet(paths.quality("qflag_breakdown")))

    # --- cleaned, harmonised daily table -----------------------------
    stations = spark.read.parquet(paths.bronze_stations).select(
        "station_id", "station_name", "country_code", "country_name",
        "latitude", "longitude", "elevation_m", "hemisphere")

    daily = to_daily(obs.filter(F.col("status") == "ok"), cfg)
    daily = daily.join(F.broadcast(stations), "station_id", "left")
    daily.write.mode("overwrite").partitionBy("year").parquet(paths.silver_daily)

    # --- per station-year coverage (data completeness) ----------------
    daily = spark.read.parquet(paths.silver_daily)
    cov = daily.groupBy("station_id", "country_code", "year").agg(
        F.count("tmax_c").alias("days_tmax"),
        F.count("tmin_c").alias("days_tmin"),
        F.count("tmean_c").alias("days_tmean"),
        F.count("prcp_mm").alias("days_prcp"),
        F.sum("n_duplicates").alias("duplicates"),
        F.sum("flag_tmin_gt_tmax").alias("tmin_gt_tmax"),
    )
    days_in_year = F.dayofyear(F.make_date(F.col("year"), F.lit(12), F.lit(31)))
    cov = (cov.withColumn("days_in_year", days_in_year)
              .withColumn("coverage_tmean", F.col("days_tmean") / F.col("days_in_year"))
              .withColumn("coverage_prcp", F.col("days_prcp") / F.col("days_in_year")))
    cov.write.mode("overwrite").parquet(paths.quality("station_year_coverage"))

    if metrics.get("_count"):
        metrics["silver_rows"] = daily.count()
        metrics["record_status"] = {
            r["status"]: r["n"] for r in spark.read.parquet(paths.quality("record_status"))
            .groupBy("status").agg(F.sum("count").alias("n")).collect()}
