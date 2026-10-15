"""Gold: business metrics derived from Silver, fully recomputed for the processed date range."""

from __future__ import annotations

import logging

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from saas_pipeline import silver
from saas_pipeline.config import table_path, to_partition_value
from saas_pipeline.storage import overwrite_date_range

log = logging.getLogger(__name__)

TABLE = "daily_metrics_by_delivery_type"


def build_daily_metrics(fact: DataFrame) -> DataFrame:
    """One row per (tenant_id, fecha_proceso, tipo_entrega).

    Revenue uses the transaction price (precio), not the catalog precio_base.
    """
    return fact.groupBy("tenant_id", "fecha_proceso", "fecha_proceso_date", "tipo_entrega").agg(
        F.sum("cantidad_normalizada_st").alias("total_units"),
        F.sum(F.col("cantidad_normalizada_st") * F.col("precio")).alias("total_revenue"),
        F.countDistinct("ruta").alias("active_routes"),
        F.countDistinct("transporte").alias("active_transports"),
    )


def run(spark: SparkSession, cfg: DictConfig, batch_id: str) -> dict[str, int]:
    start = to_partition_value(cfg.execution.start_date)
    end = to_partition_value(cfg.execution.end_date)

    fact = (
        spark.read.format("delta")
        .load(table_path(cfg, "silver", silver.FACT_TABLE))
        .filter(F.col("fecha_proceso").between(start, end))
    )
    metrics = (
        build_daily_metrics(fact)
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_processed_at", F.current_timestamp())
    )
    # ~4 rows per day and tenant: not worth physical partitions, replaceWhere works on data.
    overwrite_date_range(metrics, table_path(cfg, "gold", TABLE), start, end, partitioned=False)

    result = {"written": metrics.count()}
    log.info("[%s] gold.%s %s", cfg.tenant.code, TABLE, result)
    return result
