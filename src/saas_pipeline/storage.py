"""Delta write helpers shared by every layer."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

PARTITION_COLUMN = "fecha_proceso"


def date_range_predicate(start: str, end: str, alias: str | None = None) -> str:
    column = f"{alias}.{PARTITION_COLUMN}" if alias else PARTITION_COLUMN
    return f"{column} BETWEEN '{start}' AND '{end}'"


def overwrite_date_range(
    df: DataFrame, path: str, start: str, end: str, partitioned: bool = True
) -> None:
    """Idempotent write: replace only the fecha_proceso values in [start, end] (yyyymmdd).

    Re-running the same range rewrites those rows instead of appending duplicates;
    rows outside the range are untouched.
    """
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", date_range_predicate(start, end))
    )
    if partitioned:
        writer = writer.partitionBy(PARTITION_COLUMN)
    writer.save(path)


def overwrite_table(df: DataFrame, path: str) -> None:
    """Full overwrite, for small tables that are always rebuilt as a whole."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


def merge_into(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: tuple[str, ...],
    partition_by: str | None = None,
    scope: str | None = None,
) -> None:
    """Upsert `df` into the Delta table at `path` on the business `keys`.

    `scope` is an optional predicate on the target (alias `t`): it bounds the merge to the
    reprocessed range and deletes target rows in that range that are no longer in the source
    (e.g. a row that moved to quarantine after a fix in the rules). Without `scope` nothing is
    ever deleted, which is what a history table such as an SCD2 dimension needs.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        writer = df.write.format("delta")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.save(path)
        return

    condition = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    if scope:
        condition = f"{condition} AND {scope}"
    merge = (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
    )
    if scope:
        merge = merge.whenNotMatchedBySourceDelete(condition=scope)
    merge.execute()
