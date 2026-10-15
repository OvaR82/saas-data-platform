"""End-to-end over the real datasets for one tenant: idempotency, quality logs, fail_on_critical."""

from pathlib import Path

import pytest

from saas_pipeline import gold, quality, silver
from saas_pipeline.config import quarantine_path, table_path
from saas_pipeline.pipeline import LAYER_ORDER, run_tenant


def _counts(spark, cfg):
    paths = {
        "fact": table_path(cfg, "silver", silver.FACT_TABLE),
        "dim": table_path(cfg, "silver", silver.DIM_TABLE),
        "quarantine": quarantine_path(cfg, "silver", silver.FACT_TABLE),
        "gold": table_path(cfg, "gold", gold.TABLE),
    }
    return {name: spark.read.format("delta").load(path).count() for name, path in paths.items()}


def test_full_run_is_idempotent_and_logs_quality(spark, make_cfg):
    cfg = make_cfg()
    run_tenant(spark, cfg, list(LAYER_ORDER), "run1")
    first = _counts(spark, cfg)
    run_tenant(spark, cfg, list(LAYER_ORDER), "run2")

    assert _counts(spark, cfg) == first
    assert first["dim"] == 35  # every catalog version, history included

    run2 = spark.read.format("delta").load(cfg.paths.quality_logs).filter("_run_id = 'run2'")
    assert run2.filter("check_severity = 'critical' AND NOT check_passed").count() == 0
    assert run2.filter("check_name = 'row_reconciliation'").first().check_passed


def test_fail_on_critical_stops_before_gold(spark, make_cfg, monkeypatch):
    cfg = make_cfg("quality.fail_on_critical=true")
    broken = quality.CheckResult("silver", "fact_deliveries", "forced", "critical", 1, 1, False)
    monkeypatch.setattr(quality, "run_silver_checks", lambda *args: [broken])

    with pytest.raises(quality.CriticalQualityError):
        run_tenant(spark, cfg, list(LAYER_ORDER), "run1")

    assert not Path(table_path(cfg, "gold", gold.TABLE)).exists()
    logged = spark.read.format("delta").load(cfg.paths.quality_logs)
    assert logged.filter("check_name = 'forced'").count() == 1
