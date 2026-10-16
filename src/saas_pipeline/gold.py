"""Gold: business metrics derived from Silver, fully recomputed for the processed range.

Tables:
- daily_metrics_by_delivery_type: one row per (tenant_id, fecha_proceso, tipo_entrega).
- top_materials_by_month: the top N materials by revenue per (tenant_id, month).
"""

from __future__ import annotations

import calendar
import logging
from datetime import date

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from saas_pipeline import silver
from saas_pipeline.config import table_path, to_partition_value
from saas_pipeline.storage import overwrite_date_range, overwrite_where

log = logging.getLogger(__name__)

TABLE = "daily_metrics_by_delivery_type"
TOP_TABLE = "top_materials_by_month"


def build_daily_metrics(fact: DataFrame) -> DataFrame:
    """Aggregate Silver facts into one row per (tenant_id, fecha_proceso, tipo_entrega).

    Revenue uses the transaction price (precio), not the catalog precio_base.
    """
    # fecha_proceso_date does not change the grain (1:1 with fecha_proceso); it is kept as a
    # proper date for BI tools.
    return fact.groupBy("tenant_id", "fecha_proceso", "fecha_proceso_date", "tipo_entrega").agg(
        F.sum("cantidad_normalizada_st").alias("total_units"),
        F.sum(F.col("cantidad_normalizada_st") * F.col("precio")).alias("total_revenue"),
        F.countDistinct("ruta").alias("active_routes"),
        F.countDistinct("transporte").alias("active_transports"),
    )


def build_top_materials(fact: DataFrame, top_n: int) -> DataFrame:
    """Rank materials by revenue within each (tenant_id, month) and keep the top `top_n`.

    Adds the material's share of the month revenue. Ties are broken by material code so the
    result is deterministic.
    """
    monthly = (
        fact.withColumn("month_start", F.trunc("fecha_proceso_date", "month"))
        .groupBy("tenant_id", "month_start", "material")
        .agg(
            F.sum(F.col("cantidad_normalizada_st") * F.col("precio")).alias("total_revenue"),
            F.sum("cantidad_normalizada_st").alias("total_units"),
            # SCD2: a material can change version inside a month; report the latest one used.
            F.max_by("descripcion", "fecha_proceso").alias("descripcion"),
            F.max_by("categoria", "fecha_proceso").alias("categoria"),
        )
    )
    month = Window.partitionBy("tenant_id", "month_start")
    return (
        monthly.withColumn(
            # try_divide: a month with zero revenue gives null instead of failing under ANSI.
            "revenue_share",
            F.round(F.try_divide(F.col("total_revenue"), F.sum("total_revenue").over(month)), 4),
        )
        .withColumn(
            "revenue_rank",
            F.row_number().over(month.orderBy(F.desc("total_revenue"), "material")),
        )
        .filter(F.col("revenue_rank") <= top_n)
    )


def month_bounds(start: str, end: str) -> tuple[date, date]:
    """Expand a yyyymmdd range to whole months: (first day of start month, last day of end month).

    A monthly ranking computed from part of a month would be wrong, so the table is always
    recomputed for complete months.
    """
    first = date(int(start[:4]), int(start[4:6]), 1)
    year, month = int(end[:4]), int(end[4:6])
    last = date(year, month, calendar.monthrange(year, month)[1])
    return first, last


def _read_fact(spark: SparkSession, cfg: DictConfig, start: str, end: str) -> DataFrame:
    """Silver facts of the tenant in [start, end] (yyyymmdd). Gold only reads Silver."""
    return (
        spark.read.format("delta")
        .load(table_path(cfg, "silver", silver.FACT_TABLE))
        .filter(F.col("fecha_proceso").between(start, end))
    )


def _stamp(df: DataFrame, batch_id: str) -> DataFrame:
    """Add the technical columns of this run."""
    return df.withColumn("_batch_id", F.lit(batch_id)).withColumn(
        "_processed_at", F.current_timestamp()
    )


def run(spark: SparkSession, cfg: DictConfig, batch_id: str) -> dict[str, int]:
    """Recompute both Gold tables for one tenant and the configured range; return row counts."""
    tenant = cfg.tenant.code
    start = to_partition_value(cfg.execution.start_date)
    end = to_partition_value(cfg.execution.end_date)

    # daily_metrics_by_delivery_type: exactly the processed date range.
    # ~4 rows per day and tenant: not worth physical partitions, replaceWhere works on data.
    daily = _stamp(build_daily_metrics(_read_fact(spark, cfg, start, end)), batch_id)
    overwrite_date_range(daily, table_path(cfg, "gold", TABLE), start, end, partitioned=False)

    # top_materials_by_month: the range widened to whole months, replaced month by month.
    first, last = month_bounds(start, end)
    month_fact = _read_fact(spark, cfg, first.strftime("%Y%m%d"), last.strftime("%Y%m%d"))
    top = _stamp(build_top_materials(month_fact, cfg.gold.top_materials_n), batch_id)
    overwrite_where(
        top,
        table_path(cfg, "gold", TOP_TABLE),
        f"month_start BETWEEN DATE'{first}' AND DATE'{last.replace(day=1)}'",
    )

    result = {TABLE: daily.count(), TOP_TABLE: top.count()}
    log.info("[%s] gold %s", tenant, result)
    return result
