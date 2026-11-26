"""Stage 1 — INGEST: raw GHCN-Daily files -> typed Parquet (bronze).

Why convert first? Each yearly .csv.gz file is NOT splittable, so Spark can only
use one task per file. Parquet is columnar, compressed and splittable, so every
later stage reads only the columns it needs, in parallel on both workers.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from .config import Paths

# by_year CSV layout (no header): ID, YYYYMMDD, ELEMENT, VALUE, M-FLAG, Q-FLAG, S-FLAG, OBS-TIME
RAW_SCHEMA = StructType([
    StructField("station_id", StringType()),
    StructField("date_str", StringType()),
    StructField("element", StringType()),
    StructField("value_raw", IntegerType()),
    StructField("m_flag", StringType()),
    StructField("q_flag", StringType()),
    StructField("s_flag", StringType()),
    StructField("obs_time", StringType()),
])


def read_raw_observations(spark: SparkSession, paths: Paths, cfg: dict) -> DataFrame:
    df = (spark.read
          .option("mode", "PERMISSIVE")
          .csv(f"{paths.raw_by_year}/*.csv*", schema=RAW_SCHEMA, header=False))

    df = df.filter(F.col("element").isin(cfg["elements"]))
    if cfg.get("country_filter"):
        df = df.filter(F.substring("station_id", 1, 2).isin(cfg["country_filter"]))

    for c in ("m_flag", "q_flag", "s_flag"):          # null / " " -> ""
        df = df.withColumn(c, F.coalesce(F.trim(F.col(c)), F.lit("")))

    df = (df.filter(F.col("date_str").rlike(r"^\d{8}$"))
            .withColumn("date", F.to_date("date_str", "yyyyMMdd"))
            .filter(F.col("date").isNotNull())
            .withColumn("year", F.year("date"))
            .drop("date_str"))
    return df


def read_stations(spark: SparkSession, paths: Paths) -> DataFrame:
    """Parse the fixed-width ghcnd-stations.txt and attach the country name."""
    v = F.col("value")
    st = spark.read.text(f"{paths.raw_meta}/ghcnd-stations.txt").select(
        F.trim(F.substring(v, 1, 11)).alias("station_id"),
        F.trim(F.substring(v, 13, 8)).cast("double").alias("latitude"),
        F.trim(F.substring(v, 22, 9)).cast("double").alias("longitude"),
        F.trim(F.substring(v, 32, 6)).cast("double").alias("elevation_m"),
        F.trim(F.substring(v, 39, 2)).alias("state"),
        F.trim(F.substring(v, 42, 30)).alias("station_name"),
        F.trim(F.substring(v, 73, 3)).alias("gsn_flag"),
        F.trim(F.substring(v, 77, 3)).alias("hcn_crn_flag"),
        F.trim(F.substring(v, 81, 5)).alias("wmo_id"),
    ).filter(F.length("station_id") == 11)

    st = (st.withColumn("elevation_m", F.when(F.col("elevation_m") > -999, F.col("elevation_m")))
            .withColumn("country_code", F.substring("station_id", 1, 2))
            .withColumn("hemisphere", F.when(F.col("latitude") >= 0, "N").otherwise("S")))

    countries = spark.read.text(f"{paths.raw_meta}/ghcnd-countries.txt").select(
        F.substring("value", 1, 2).alias("country_code"),
        F.trim(F.expr("substring(value, 4)")).alias("country_name"),
    )
    return st.join(F.broadcast(countries), "country_code", "left")


def run(spark: SparkSession, paths: Paths, cfg: dict, metrics: dict) -> None:
    obs = read_raw_observations(spark, paths, cfg)
    (obs.write.mode("overwrite")
        .partitionBy("year")
        .parquet(paths.bronze_obs))

    stations = read_stations(spark, paths)
    stations.coalesce(1).write.mode("overwrite").parquet(paths.bronze_stations)

    if metrics.get("_count"):
        metrics["bronze_rows"] = spark.read.parquet(paths.bronze_obs).count()
        metrics["stations"] = spark.read.parquet(paths.bronze_stations).count()
