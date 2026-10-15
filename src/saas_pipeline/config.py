"""Hierarchical configuration with OmegaConf.

Precedence (low -> high): config/base.yaml -> config/env/<env>.yaml
-> config/tenants/<tenant>.yaml -> CLI overrides (dotlist, e.g. quality.fail_on_critical=true).
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from omegaconf import DictConfig, OmegaConf

ENVIRONMENTS = ("dev", "qa", "main")
DEFAULT_CONFIG_DIR = Path(os.environ.get("SAAS_CONFIG_DIR", "config"))


class ConfigError(ValueError):
    """Raised when the configuration is missing or inconsistent."""


def available_tenants(config_dir: Path = DEFAULT_CONFIG_DIR) -> list[str]:
    """Tenants are onboarded by adding config/tenants/<code>.yaml (enabled: true)."""
    tenants = []
    for path in sorted((config_dir / "tenants").glob("*.yaml")):
        if OmegaConf.load(path).tenant.get("enabled", True):
            tenants.append(path.stem)
    return tenants


def resolve_tenants(selection: str, config_dir: Path = DEFAULT_CONFIG_DIR) -> list[str]:
    """Translate the --tenant argument ('all' or a code) into a list of tenant codes."""
    tenants = available_tenants(config_dir)
    if selection == "all":
        return tenants
    if selection not in tenants:
        raise ConfigError(f"Unknown or disabled tenant '{selection}'. Available: {tenants}")
    return [selection]


def load_config(
    env: str,
    tenant: str,
    overrides: list[str] | None = None,
    config_dir: Path = DEFAULT_CONFIG_DIR,
) -> DictConfig:
    """Merge every config level for one (env, tenant) and return a resolved, read-only config."""
    if env not in ENVIRONMENTS:
        raise ConfigError(f"Unknown env '{env}'. Expected one of {ENVIRONMENTS}")
    tenant_file = config_dir / "tenants" / f"{tenant}.yaml"
    if not tenant_file.exists():
        raise ConfigError(f"Tenant config not found: {tenant_file}")

    cfg = OmegaConf.merge(
        OmegaConf.load(config_dir / "base.yaml"),
        OmegaConf.load(config_dir / "env" / f"{env}.yaml"),
        OmegaConf.load(tenant_file),
        OmegaConf.from_dotlist(overrides or []),
    )
    OmegaConf.resolve(cfg)
    _validate(cfg, tenant)
    OmegaConf.set_readonly(cfg, True)
    return cfg


def _validate(cfg: DictConfig, tenant: str) -> None:
    if cfg.tenant.code != tenant or cfg.tenant.code != cfg.tenant.code.lower():
        raise ConfigError(f"tenant.code must be lowercase and match the file name '{tenant}'")
    try:
        start = date.fromisoformat(cfg.execution.start_date)
        end = date.fromisoformat(cfg.execution.end_date)
    except ValueError as exc:
        raise ConfigError(f"Dates must use YYYY-MM-DD: {exc}") from exc
    if start > end:
        raise ConfigError(f"start_date {start} is after end_date {end}")


def to_partition_value(iso_date: str) -> str:
    """'2025-03-14' -> '20250314' (format of fecha_proceso)."""
    return date.fromisoformat(iso_date).strftime("%Y%m%d")


def table_path(cfg: DictConfig, layer: str, table: str) -> str:
    """<paths.layer>/<tenant>/<table>, e.g. data/silver/sv/fact_deliveries."""
    return f"{cfg.paths[layer]}/{cfg.tenant.code}/{table}"


def quarantine_path(cfg: DictConfig, layer: str, table: str) -> str:
    """<quarantine_root>/<layer>_quarantine/<tenant>/<table>."""
    return f"{cfg.paths.quarantine_root}/{layer}_quarantine/{cfg.tenant.code}/{table}"
