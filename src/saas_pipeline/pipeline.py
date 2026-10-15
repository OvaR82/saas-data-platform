"""Orchestration: runs the requested layers for each tenant, honoring fail_fast."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

from omegaconf import DictConfig, OmegaConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline import bronze, gold, quality, silver
from saas_pipeline.config import (
    DEFAULT_CONFIG_DIR,
    load_config,
    resolve_tenants,
    table_path,
    to_partition_value,
)
from saas_pipeline.session import get_spark

log = logging.getLogger(__name__)

LAYER_ORDER = ("bronze", "silver", "gold")


def _silver_checks(
    spark: SparkSession, cfg: DictConfig, metrics: dict[str, int]
) -> list[quality.CheckResult]:
    start = to_partition_value(cfg.execution.start_date)
    end = to_partition_value(cfg.execution.end_date)
    fact = (
        spark.read.format("delta")
        .load(table_path(cfg, "silver", silver.FACT_TABLE))
        .filter(F.col("fecha_proceso").between(start, end))
    )
    dim = spark.read.format("delta").load(table_path(cfg, "silver", silver.DIM_TABLE))
    return quality.run_silver_checks(fact, dim, metrics, cfg.quality.max_quarantine_rate)


def run_tenant(spark: SparkSession, cfg: DictConfig, layers: list[str], run_id: str) -> None:
    tenant = cfg.tenant.code
    batch_id = f"{run_id}_{tenant}"
    checks: list[quality.CheckResult] = []
    try:
        if "bronze" in layers:
            checks.append(quality.check_bronze_process_date(bronze.run(spark, cfg, batch_id)))
        if "silver" in layers:
            checks += _silver_checks(spark, cfg, silver.run(spark, cfg, batch_id))
    finally:
        # Persist whatever was checked, even if a later step failed.
        quality.write_quality_logs(spark, checks, cfg.paths.quality_logs, run_id, batch_id, tenant)

    # Raises CriticalQualityError (tenant fails, Gold is skipped) if configured to.
    quality.enforce(checks, tenant, cfg.quality.fail_on_critical)
    if "gold" in layers:
        gold.run(spark, cfg, batch_id)


def run(
    env: str,
    tenant_selection: str | None,
    layers: list[str],
    overrides: list[str] | None = None,
    config_dir: Path = DEFAULT_CONFIG_DIR,
) -> int:
    """Run the pipeline; returns a process exit code (0 = every tenant succeeded)."""
    run_id = uuid.uuid4().hex[:12]
    if tenant_selection is None:
        tenant_selection = OmegaConf.load(config_dir / "base.yaml").execution.tenant
    tenants = resolve_tenants(tenant_selection, config_dir)
    # Load every tenant config up front: a config error should fail before touching data.
    configs = {t: load_config(env, t, overrides, config_dir) for t in tenants}
    spark = get_spark()
    log.info("run_id=%s env=%s tenants=%s layers=%s", run_id, env, tenants, layers)

    failed: list[str] = []
    for tenant, cfg in configs.items():
        try:
            run_tenant(spark, cfg, layers, run_id)
        except Exception:
            log.exception("Tenant %s failed", tenant)
            failed.append(tenant)
            if cfg.execution.fail_fast:
                log.error("fail_fast=true: aborting the remaining tenants")
                break

    if failed:
        log.error("run_id=%s finished with failures: %s", run_id, failed)
        return 1
    log.info("run_id=%s finished OK", run_id)
    return 0
