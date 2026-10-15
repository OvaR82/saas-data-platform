"""SparkSession factory: reuses the active session on Databricks, builds a local one otherwise."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "saas-pipeline") -> SparkSession:
    """Return the SparkSession to use, with Delta Lake enabled and UTC/strict-date settings."""
    # On Databricks (or inside tests) a session already exists and must be reused.
    spark = SparkSession.getActiveSession()
    if spark is None:
        # Imported lazily: only the local run needs delta-spark to fetch the Delta jars.
        from delta import configure_spark_with_delta_pip

        # Local session with the Delta extensions (MERGE, replaceWhere, time travel).
        builder = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.shuffle.partitions", "8")  # small dataset, avoid 200 tiny tasks
            .config("spark.ui.showConsoleProgress", "false")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

    # Technical timestamps in UTC (naming convention 5.3) and strict date parsing:
    # with CORRECTED an impossible date like 20250230 parses to null instead of raising.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    return spark
