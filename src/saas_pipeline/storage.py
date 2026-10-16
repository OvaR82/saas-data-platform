"""Delta write helpers shared by every layer."""

from __future__ import annotations

from pyspark.sql import DataFrame

PARTITION_COLUMN = "fecha_proceso"


def overwrite_date_range(df: DataFrame, path: str, start: str, end: str) -> None:
    """Idempotent write: replace only the fecha_proceso partitions in [start, end] (yyyymmdd).

    Re-running the same range rewrites those partitions instead of appending duplicates;
    partitions outside the range are untouched.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{PARTITION_COLUMN} BETWEEN '{start}' AND '{end}'")
        .partitionBy(PARTITION_COLUMN)
        .save(path)
    )


def overwrite_table(df: DataFrame, path: str) -> None:
    """Full overwrite, for small tables that are always rebuilt as a whole."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
