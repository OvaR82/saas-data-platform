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
    """Every tenant YAML loads and validates in every environment."""
    for tenant in available_tenants(CONFIG_DIR):
        cfg = load_config(env, tenant, config_dir=CONFIG_DIR)
        assert cfg.tenant.code == tenant


def test_paths_change_per_env_and_follow_architecture(monkeypatch):
    """Default paths follow architecture 5.2 and differ per environment."""
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
    """SAAS_DATA_ROOT relocates the generated layers but not the RAW inputs."""
    monkeypatch.setenv("SAAS_DATA_ROOT", "/tmp/saas-data")
    dev = load_config("dev", "sv", config_dir=CONFIG_DIR)
    main = load_config("main", "sv", config_dir=CONFIG_DIR)
    assert table_path(dev, "gold", "x") == "/tmp/saas-data/gold/sv/x"
    assert dev.paths.quality_logs == "/tmp/saas-data/shared/quality_logs"
    assert table_path(main, "gold", "x") == "/tmp/saas-data/main/gold/sv/x"
    assert dev.paths.raw == "data/raw"  # inputs stay in the repository


def test_cli_overrides_have_highest_precedence():
    """CLI overrides beat base, env and tenant files."""
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
    """Bad env, unknown tenant and an inverted date range raise ConfigError."""
    # Unknown environment.
    with pytest.raises(ConfigError):
        load_config("prod", "sv", config_dir=CONFIG_DIR)
    # Tenant without a YAML file.
    with pytest.raises(ConfigError):
        resolve_tenants("xx", CONFIG_DIR)
    # start_date after the default end_date (2025-06-30).
    with pytest.raises(ConfigError):
        load_config("dev", "sv", ["execution.start_date=2025-07-01"], config_dir=CONFIG_DIR)


@pytest.mark.parametrize(
    "value", ["20250301", "2025-W10-1", "2025-3-1", "01/03/2025", "2025-02-30"]
)
def test_dates_must_be_real_and_strictly_yyyy_mm_dd(value):
    """Only YYYY-MM-DD calendar dates are accepted (5.8), with a clear ConfigError otherwise.

    20250301 is the most likely mistake (it is the fecha_proceso format): OmegaConf parses it as
    an int, which used to crash with a TypeError instead of a readable message.
    """
    with pytest.raises(ConfigError):
        load_config("dev", "sv", [f"execution.start_date={value}"], config_dir=CONFIG_DIR)


def test_partition_value_format():
    """Config dates are converted to the fecha_proceso partition format."""
    assert to_partition_value("2025-03-14") == "20250314"


def test_cli_reports_config_errors_without_a_traceback(caplog):
    """A bad --start-date ends with a readable message and exit code 2 (before starting Spark)."""
    from saas_pipeline.cli import main

    assert main(["run", "--tenant", "sv", "--start-date", "20250301"]) == 2
    assert "Dates must use YYYY-MM-DD" in caplog.text
