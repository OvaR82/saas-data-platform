"""Hierarchical configuration: loading, precedence, paths and validation (no Spark needed)."""

from pathlib import Path

import pytest

from saas_pipeline.config import (
    ENVIRONMENTS,
    ConfigError,
    available_tenants,
    load_config,
    quarantine_path,
    resolve_tenants,
    table_path,
    to_partition_value,
)

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


@pytest.mark.parametrize("env", ENVIRONMENTS)
def test_every_env_and_tenant_loads(env):
    for tenant in available_tenants(CONFIG_DIR):
        cfg = load_config(env, tenant, config_dir=CONFIG_DIR)
        assert cfg.tenant.code == tenant


def test_paths_change_per_env_and_follow_architecture(monkeypatch):
    # Ignore a SAAS_DATA_ROOT set on the developer's machine: assert the defaults.
    monkeypatch.delenv("SAAS_DATA_ROOT", raising=False)
    dev = load_config("dev", "sv", config_dir=CONFIG_DIR)
    qa = load_config("qa", "sv", config_dir=CONFIG_DIR)
    assert table_path(dev, "bronze", "deliveries") == "data/bronze/sv/deliveries"
    assert table_path(qa, "bronze", "deliveries") == "data/qa/bronze/sv/deliveries"
    assert quarantine_path(dev, "silver", "fact_deliveries") == (
        "data/silver_quarantine/sv/fact_deliveries"
    )
    assert dev.paths.raw == qa.paths.raw  # RAW input is shared across environments


def test_data_root_can_be_moved_with_an_environment_variable(monkeypatch):
    monkeypatch.setenv("SAAS_DATA_ROOT", "/tmp/saas-data")
    dev = load_config("dev", "sv", config_dir=CONFIG_DIR)
    main = load_config("main", "sv", config_dir=CONFIG_DIR)
    assert table_path(dev, "gold", "x") == "/tmp/saas-data/gold/sv/x"
    assert dev.paths.quality_logs == "/tmp/saas-data/shared/quality_logs"
    assert table_path(main, "gold", "x") == "/tmp/saas-data/main/gold/sv/x"
    assert dev.paths.raw == "data/raw"  # inputs stay in the repository


def test_cli_overrides_have_highest_precedence():
    # main.yaml sets fail_on_critical=true; the CLI override must win.
    cfg = load_config(
        "main",
        "sv",
        ["quality.fail_on_critical=false", "execution.start_date=2025-03-01"],
        config_dir=CONFIG_DIR,
    )
    assert cfg.quality.fail_on_critical is False
    assert cfg.execution.start_date == "2025-03-01"


def test_invalid_inputs_are_rejected():
    # Unknown environment.
    with pytest.raises(ConfigError):
        load_config("prod", "sv", config_dir=CONFIG_DIR)
    # Tenant without a YAML file.
    with pytest.raises(ConfigError):
        resolve_tenants("xx", CONFIG_DIR)
    # start_date after the default end_date (2025-06-30).
    with pytest.raises(ConfigError):
        load_config("dev", "sv", ["execution.start_date=2025-07-01"], config_dir=CONFIG_DIR)


def test_partition_value_format():
    assert to_partition_value("2025-03-14") == "20250314"
