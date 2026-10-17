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
    quality_logs_ref,
    resolve_tenants,
    table_ref,
    to_partition_value,
)
from saas_pipeline.session import get_spark
from saas_pipeline.storage import read_table

log = logging.getLogger(__name__)

# Execution order: each layer only reads the one before it.
LAYER_ORDER = ("bronze", "silver", "gold")


def _silver_checks(
    spark: SparkSession, cfg: DictConfig, metrics: dict[str, int]
) -> list[quality.CheckResult]:
    """Run the Silver checks on what was actually persisted, not on in-memory DataFrames."""
    start = to_partition_value(cfg.execution.start_date)
    end = to_partition_value(cfg.execution.end_date)
    fact = gold.read_fact(spark, cfg, start, end)
    dim = read_table(spark, table_ref(cfg, "silver", silver.DIM_TABLE))
    return quality.run_silver_checks(fact, dim, metrics, cfg.quality.max_quarantine_rate)


def _gold_checks(spark: SparkSession, cfg: DictConfig) -> list[quality.CheckResult]:
    """Compare the persisted Gold daily metrics with Silver for the processed range."""
    start = to_partition_value(cfg.execution.start_date)
    end = to_partition_value(cfg.execution.end_date)
    daily = read_table(spark, table_ref(cfg, "gold", gold.TABLE)).filter(
        F.col("fecha_proceso").between(start, end)
    )
    return [
        quality.check_gold_reconciles_with_silver(gold.read_fact(spark, cfg, start, end), daily)
    ]


def run_tenant(spark: SparkSession, cfg: DictConfig, layers: list[str], run_id: str) -> None:
    """Bronze -> Silver -> quality gate -> Gold -> Gold checks for a single tenant."""
    tenant = cfg.tenant.code
    # run_id identifies the whole execution; batch_id the work of one tenant inside it.
    batch_id = f"{run_id}_{tenant}"
    checks: list[quality.CheckResult] = []
    try:
        if "bronze" in layers:
            checks.append(quality.check_bronze_process_date(bronze.run(spark, cfg, batch_id)))
        if "silver" in layers:
            checks += _silver_checks(spark, cfg, silver.run(spark, cfg, batch_id))
    finally:
        # Persist whatever was checked, even if a later step failed.
        quality.write_quality_logs(spark, checks, quality_logs_ref(cfg), run_id, batch_id, tenant)

    # Raises CriticalQualityError (tenant fails, Gold is skipped) if configured to.
    quality.enforce(checks, tenant, cfg.quality.fail_on_critical)
    if "gold" in layers:
        gold.run(spark, cfg, batch_id)
        # Gold checks run on what was published: they detect a bad aggregation (and fail the
        # tenant with fail_on_critical), but cannot prevent publishing it.
        gold_checks = _gold_checks(spark, cfg)
        quality.write_quality_logs(
            spark, gold_checks, quality_logs_ref(cfg), run_id, batch_id, tenant
        )
        quality.enforce(gold_checks, tenant, cfg.quality.fail_on_critical)


def run(
    env: str,
    tenant_selection: str | None,
    layers: list[str],
    overrides: list[str] | None = None,
    config_dir: Path = DEFAULT_CONFIG_DIR,
) -> int:
    """Run the pipeline; returns a process exit code (0 = every tenant succeeded)."""
    run_id = uuid.uuid4().hex[:12]
    # Without --tenant, fall back to execution.tenant from base.yaml.
    if tenant_selection is None:
        tenant_selection = OmegaConf.load(config_dir / "base.yaml").execution.tenant
    tenants = resolve_tenants(tenant_selection, config_dir)
    # Load every tenant config up front: a config error should fail before touching data.
    configs = {t: load_config(env, t, overrides, config_dir) for t in tenants}
    spark = get_spark()
    log.info("run_id=%s env=%s tenants=%s layers=%s", run_id, env, tenants, layers)

    # Tenants are isolated: one failure is logged and, unless fail_fast, the others continue.
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

    # Summary at the end, plus a non-zero exit code so a scheduler marks the job as failed.
    if failed:
        log.error("run_id=%s finished with failures: %s", run_id, failed)
        return 1
    log.info("run_id=%s finished OK", run_id)
    return 0
