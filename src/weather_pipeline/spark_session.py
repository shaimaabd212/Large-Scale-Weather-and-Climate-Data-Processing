"""SparkSession factory used by all stages."""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "weather-climate-pipeline", local: bool = False) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if local:
        builder = (builder.master("local[*]")
                   .config("spark.sql.shuffle.partitions", "8")
                   .config("spark.driver.memory", "4g"))
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def cluster_info(spark: SparkSession) -> dict:
    sc = spark.sparkContext
    try:
        executors = sc._jsc.sc().getExecutorMemoryStatus().size() - 1  # minus the driver
    except Exception:  # pragma: no cover
        executors = None
    return {
        "master": sc.master,
        "app_id": sc.applicationId,
        "spark_version": spark.version,
        "default_parallelism": sc.defaultParallelism,
        "executors": executors,
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
    }
