"""Hierarchical configuration with OmegaConf.

Precedence (low -> high): config/base.yaml -> config/env/<env>.yaml
-> config/tenants/<tenant>.yaml -> CLI overrides (dotlist, e.g. quality.fail_on_critical=true).
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from omegaconf import DictConfig, OmegaConf

from saas_pipeline.storage import TableRef

# Supported environments; each one has its overrides in config/env/<env>.yaml.
ENVIRONMENTS = ("dev", "qa", "main")
# path: Delta tables by path (layout of architecture 5.2); catalog: Unity Catalog table names.
STORAGE_MODES = ("path", "catalog")
# Overridable so the package can run from another working directory (e.g. a Databricks job).
DEFAULT_CONFIG_DIR = Path(os.environ.get("SAAS_CONFIG_DIR", "config"))


class ConfigError(ValueError):
    """Raised when the configuration is missing or inconsistent."""


def available_tenants(config_dir: Path = DEFAULT_CONFIG_DIR) -> list[str]:
    """List the enabled tenant codes.

    A tenant is onboarded by adding config/tenants/<code>.yaml (enabled: true).
    """
    tenants = []
    for path in sorted((config_dir / "tenants").glob("*.yaml")):
        # A disabled tenant keeps its YAML but is skipped by --tenant all.
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
    # Fail early with a clear message instead of a missing-file error deep inside OmegaConf.
    if env not in ENVIRONMENTS:
        raise ConfigError(f"Unknown env '{env}'. Expected one of {ENVIRONMENTS}")
    tenant_file = config_dir / "tenants" / f"{tenant}.yaml"
    if not tenant_file.exists():
        raise ConfigError(f"Tenant config not found: {tenant_file}")

    # Later levels win: base < env < tenant < CLI overrides.
    cfg = OmegaConf.merge(
        OmegaConf.load(config_dir / "base.yaml"),
        OmegaConf.load(config_dir / "env" / f"{env}.yaml"),
        OmegaConf.load(tenant_file),
        OmegaConf.from_dotlist(overrides or []),
    )
    # Resolve ${...} interpolations (paths.root, SAAS_DATA_ROOT) only after every level merged,
    # so an override of paths.root also moves bronze/silver/gold/quality_logs.
    OmegaConf.resolve(cfg)
    _validate(cfg, tenant)
    # Read-only: no layer can change the configuration in the middle of a run.
    OmegaConf.set_readonly(cfg, True)
    return cfg


def _validate(cfg: DictConfig, tenant: str) -> None:
    """Consistency rules that a valid YAML file does not guarantee."""
    # Tenant codes are lowercase everywhere (paths, schemas, _tenant_id), per naming rule 5.3.
    if cfg.tenant.code != tenant or cfg.tenant.code != cfg.tenant.code.lower():
        raise ConfigError(f"tenant.code must be lowercase and match the file name '{tenant}'")
    # The date range must be parseable and ordered before it is used to filter partitions.
    try:
        start = date.fromisoformat(cfg.execution.start_date)
        end = date.fromisoformat(cfg.execution.end_date)
    except ValueError as exc:
        raise ConfigError(f"Dates must use YYYY-MM-DD: {exc}") from exc
    if start > end:
        raise ConfigError(f"start_date {start} is after end_date {end}")
    # Tables are addressed either by path (this test) or by Unity Catalog name (Databricks).
    if cfg.storage.mode not in STORAGE_MODES:
        raise ConfigError(f"storage.mode must be one of {STORAGE_MODES}, got '{cfg.storage.mode}'")


def to_partition_value(iso_date: str) -> str:
    """Convert a config date to the fecha_proceso format: '2025-03-14' -> '20250314'."""
    return date.fromisoformat(iso_date).strftime("%Y%m%d")


def table_path(cfg: DictConfig, layer: str, table: str) -> str:
    """Build the path of a tenant table: <paths.layer>/<tenant>/<table>.

    Example: data/silver/sv/fact_deliveries.
    """
    return f"{cfg.paths[layer]}/{cfg.tenant.code}/{table}"


def quarantine_path(cfg: DictConfig, layer: str, table: str) -> str:
    """Build the path of a quarantine table.

    Layout: <quarantine_root>/<layer>_quarantine/<tenant>/<table>.
    """
    return f"{cfg.paths.quarantine_root}/{layer}_quarantine/{cfg.tenant.code}/{table}"


# Logical names follow architecture 5.2 / 5.6 / 5.9: the same names in both modes.


def table_ref(cfg: DictConfig, layer: str, table: str) -> TableRef:
    """A tenant table: <paths.layer>/<tenant>/<table> or <catalog>.<layer>_<tenant>.<table>."""
    if cfg.storage.mode == "catalog":
        return TableRef(name=f"{cfg.storage.catalog}.{layer}_{cfg.tenant.code}.{table}")
    return TableRef(path=table_path(cfg, layer, table))


def quarantine_ref(cfg: DictConfig, layer: str, table: str) -> TableRef:
    """A quarantine table: the quarantine path or <catalog>.<layer>_quarantine_<tenant>.<table>."""
    if cfg.storage.mode == "catalog":
        return TableRef(name=f"{cfg.storage.catalog}.{layer}_quarantine_{cfg.tenant.code}.{table}")
    return TableRef(path=quarantine_path(cfg, layer, table))


def quality_logs_ref(cfg: DictConfig) -> TableRef:
    """The shared quality_logs table: paths.quality_logs or <catalog>.shared.quality_logs."""
    if cfg.storage.mode == "catalog":
        return TableRef(name=f"{cfg.storage.catalog}.shared.quality_logs")
    return TableRef(path=cfg.paths.quality_logs)
