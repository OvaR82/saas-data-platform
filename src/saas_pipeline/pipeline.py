"""Orchestration: runs the requested layers for each tenant, honoring fail_fast."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

from omegaconf import DictConfig, OmegaConf
from pyspark.sql import SparkSession

from saas_pipeline import bronze
from saas_pipeline.config import DEFAULT_CONFIG_DIR, load_config, resolve_tenants
from saas_pipeline.session import get_spark

log = logging.getLogger(__name__)

LAYER_ORDER = ("bronze",)


def run_tenant(spark: SparkSession, cfg: DictConfig, layers: list[str], run_id: str) -> None:
    batch_id = f"{run_id}_{cfg.tenant.code}"
    if "bronze" in layers:
        bronze.run(spark, cfg, batch_id)


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
