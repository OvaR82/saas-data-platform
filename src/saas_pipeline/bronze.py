"""Bronze: RAW CSV -> Delta, original columns + technical columns, partitioned by fecha_proceso.

Values are kept as strings (as delivered by the source); typing and business rules belong to
Silver. Rows whose fecha_proceso cannot be parsed cannot be assigned to a partition, so they go
to bronze_quarantine instead of being dropped (see docs/observations.md).
"""

from __future__ import annotations

import logging

from omegaconf import DictConfig
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from saas_pipeline.config import quarantine_path, table_path, to_partition_value
from saas_pipeline.storage import overwrite_date_range, overwrite_table

log = logging.getLogger(__name__)

TABLE = "deliveries"
SOURCE_COLUMNS = (
    "pais",
    "fecha_proceso",
    "transporte",
    "ruta",
    "tipo_entrega",
    "material",
    "precio",
    "cantidad",
    "unidad",
)
RAW_SCHEMA = StructType([StructField(name, StringType()) for name in SOURCE_COLUMNS])


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read the source CSV as strings; enforceSchema=False fails if the header changes."""
    return (
        spark.read.option("header", True)
        .option("enforceSchema", False)
        .schema(RAW_SCHEMA)
        .csv(path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def is_valid_process_date(col: Column) -> Column:
    """True only for real calendar dates in yyyyMMdd (rejects null, '2025-03-15', '20250230')."""
    parsed = F.to_date(col, "yyyyMMdd")
    is_valid = col.rlike(r"^\d{8}$") & (F.date_format(parsed, "yyyyMMdd") == col)
    return F.coalesce(is_valid, F.lit(False))


def add_technical_columns(df: DataFrame, tenant: str, batch_id: str) -> DataFrame:
    """Keep the tenant's rows, normalize pais to lowercase and add the technical columns."""
    return (
        df.withColumn("pais", F.lower(F.trim("pais")))
        .filter(F.col("pais") == tenant)
        .withColumn("_tenant_id", F.col("pais"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def split_by_process_date(df: DataFrame, start: str, end: str) -> tuple[DataFrame, DataFrame]:
    """Return (rows in [start, end] with a valid date, rows with a null/invalid date)."""
    valid = is_valid_process_date(F.col("fecha_proceso"))
    in_range = df.filter(valid & F.col("fecha_proceso").between(start, end))
    quarantine = df.filter(~valid).withColumn(
        "_quarantine_reason",
        F.when(F.col("fecha_proceso").isNull(), "null_fecha_proceso").otherwise(
            "invalid_fecha_proceso"
        ),
    )
    return in_range, quarantine


def run(spark: SparkSession, cfg: DictConfig, batch_id: str) -> dict[str, int]:
    tenant = cfg.tenant.code
    start = to_partition_value(cfg.execution.start_date)
    end = to_partition_value(cfg.execution.end_date)

    raw = read_raw(spark, f"{cfg.paths.raw}/{cfg.sources.deliveries}")
    # Cached: read once for both writes and the counts (also keeps one _ingestion_timestamp).
    tenant_rows = add_technical_columns(raw, tenant, batch_id).cache()
    rows, quarantine = split_by_process_date(tenant_rows, start, end)

    overwrite_date_range(rows, table_path(cfg, "bronze", TABLE), start, end)
    # Undated rows do not belong to any date range, so the tenant's bronze quarantine is rebuilt
    # as a whole on each run (still idempotent).
    overwrite_table(quarantine, quarantine_path(cfg, "bronze", TABLE))

    metrics = {"written": rows.count(), "quarantined": quarantine.count()}
    tenant_rows.unpersist()
    log.info("[%s] bronze.%s %s", tenant, TABLE, metrics)
    return metrics
