"""Delta read/write helpers shared by every layer.

Every table is addressed with a TableRef: a filesystem path (storage.mode=path, the layout of
architecture 5.2 used in this test) or a Unity Catalog name `catalog.schema.table`
(storage.mode=catalog, the Databricks mapping). The layers never know which one they use.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

# Every date-ranged table is filtered and partitioned by the source process date (yyyymmdd).
PARTITION_COLUMN = "fecha_proceso"


@dataclass(frozen=True)
class TableRef:
    """Where a Delta table lives: exactly one of `path` or `name` (catalog.schema.table)."""

    path: str | None = None
    name: str | None = None

    def __post_init__(self) -> None:
        if (self.path is None) == (self.name is None):
            raise ValueError("TableRef needs exactly one of path or name")

    def __str__(self) -> str:
        return self.name or self.path


def read_table(spark: SparkSession, ref: TableRef) -> DataFrame:
    """Read a Delta table by name or by path."""
    if ref.name:
        return spark.read.table(ref.name)
    return spark.read.format("delta").load(ref.path)


def table_exists(spark: SparkSession, ref: TableRef) -> bool:
    """True if the Delta table already exists (by name in the catalog, or as a Delta path)."""
    if ref.name:
        return spark.catalog.tableExists(ref.name)
    return DeltaTable.isDeltaTable(spark, ref.path)


def _save(spark: SparkSession, writer: DataFrameWriter, ref: TableRef) -> None:
    """Finish a write: saveAsTable for a catalog name (creating its schema), save for a path."""
    if ref.name:
        # In production the schemas come from Terraform (docs/infra.md) and this is a no-op;
        # locally (spark_catalog) it creates them.
        schema = ref.name.rsplit(".", 1)[0]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        writer.saveAsTable(ref.name)
    else:
        writer.save(ref.path)


def date_range_predicate(start: str, end: str, alias: str | None = None) -> str:
    """SQL predicate for the processed range; `alias` qualifies the column inside a MERGE."""
    column = f"{alias}.{PARTITION_COLUMN}" if alias else PARTITION_COLUMN
    return f"{column} BETWEEN '{start}' AND '{end}'"


def overwrite_date_range(
    df: DataFrame, ref: TableRef, start: str, end: str, partitioned: bool = True
) -> None:
    """Write `df` replacing only the fecha_proceso values in [start, end] (yyyymmdd).

    Re-running the same range rewrites those rows instead of appending duplicates;
    rows outside the range are untouched.
    """
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", date_range_predicate(start, end))
    )
    # Small tables (Gold) stay unpartitioned: replaceWhere also works on plain data columns.
    if partitioned:
        writer = writer.partitionBy(PARTITION_COLUMN)
    _save(df.sparkSession, writer, ref)


def overwrite_where(df: DataFrame, ref: TableRef, predicate: str) -> None:
    """Write `df` replacing only the target rows that match `predicate` (any column).

    Used when the replaced range is not a fecha_proceso range, e.g. whole months.
    """
    writer = df.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
    _save(df.sparkSession, writer, ref)


def overwrite_table(df: DataFrame, ref: TableRef) -> None:
    """Replace the whole table; for small tables that are always rebuilt as a whole."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    _save(df.sparkSession, writer, ref)


def append_table(df: DataFrame, ref: TableRef) -> None:
    """Append rows (audit trails such as quality_logs); creates the table if needed."""
    _save(df.sparkSession, df.write.format("delta").mode("append"), ref)


def merge_into(
    spark: SparkSession,
    df: DataFrame,
    ref: TableRef,
    keys: tuple[str, ...],
    partition_by: str | None = None,
    scope: str | None = None,
) -> None:
    """Upsert `df` into the Delta table `ref` on the business `keys`.

    `scope` is an optional predicate on the target (alias `t`): it bounds the merge to the
    reprocessed range and deletes target rows in that range that are no longer in the source
    (e.g. a row that moved to quarantine after a fix in the rules). Without `scope` nothing is
    ever deleted, which is what a history table such as an SCD2 dimension needs.
    """
    # First run: there is nothing to merge into, so the table is simply created.
    if not table_exists(spark, ref):
        writer = df.write.format("delta")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        _save(spark, writer, ref)
        return

    # Match on the business key; adding the scope lets Delta prune the untouched partitions.
    condition = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    if scope:
        condition = f"{condition} AND {scope}"
    # SQL MERGE INTO (as named in architecture 5.5) accepts both a three-part catalog name and
    # a path (delta.`...`); the Python DeltaTable.forName of OSS Delta only takes two parts.
    target = ref.name or f"delta.`{ref.path}`"
    source = f"_merge_source_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(source)
    statement = f"""
        MERGE INTO {target} AS t
        USING {source} AS s
        ON {condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    # Keys that disappeared from the source inside the scope are removed (exact reprocessing).
    if scope:
        statement += f"WHEN NOT MATCHED BY SOURCE AND {scope} THEN DELETE"
    try:
        spark.sql(statement)
    finally:
        spark.catalog.dropTempView(source)
