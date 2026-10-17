"""End-to-end over the real datasets for one tenant: idempotency, quality logs and the gate."""

from pathlib import Path

import pytest

from saas_pipeline import gold, quality, silver
from saas_pipeline.config import quarantine_path, table_path
from saas_pipeline.pipeline import LAYER_ORDER, run_tenant


def _counts(spark, cfg):
    """Row count of every table a tenant run writes (except Bronze, covered elsewhere)."""
    paths = {
        "fact": table_path(cfg, "silver", silver.FACT_TABLE),
        "dim": table_path(cfg, "silver", silver.DIM_TABLE),
        "quarantine": quarantine_path(cfg, "silver", silver.FACT_TABLE),
        "gold": table_path(cfg, "gold", gold.TABLE),
        "gold_top": table_path(cfg, "gold", gold.TOP_TABLE),
    }
    return {name: spark.read.format("delta").load(path).count() for name, path in paths.items()}


def test_full_run_is_idempotent_and_logs_quality(spark, make_cfg):
    """A full tenant run is idempotent and leaves passing critical checks in quality_logs."""
    cfg = make_cfg()
    # Two full runs over the same range must leave identical tables.
    run_tenant(spark, cfg, list(LAYER_ORDER), "run1")
    first = _counts(spark, cfg)
    run_tenant(spark, cfg, list(LAYER_ORDER), "run2")

    assert _counts(spark, cfg) == first
    assert first["dim"] == 35  # every catalog version, history included
    # dim_materials is built from Bronze (5.1) and keeps the lineage of the catalog file.
    dim = spark.read.format("delta").load(table_path(cfg, "silver", silver.DIM_TABLE))
    assert dim.first()._source_file.endswith("materials_catalog.csv")

    # quality_logs is append-only: look only at the second run.
    run2 = spark.read.format("delta").load(cfg.paths.quality_logs).filter("_run_id = 'run2'")
    assert run2.filter("check_severity = 'critical' AND NOT check_passed").count() == 0
    assert run2.filter("check_name = 'row_reconciliation'").first().check_passed
    assert run2.filter("check_name = 'gold_reconciles_with_silver'").first().check_passed


def test_fail_on_critical_stops_before_gold(spark, make_cfg, monkeypatch):
    """With fail_on_critical, a failed critical check stops the tenant before Gold."""
    cfg = make_cfg("quality.fail_on_critical=true")
    # Force a failing critical check instead of corrupting real data.
    broken = quality.CheckResult("silver", "fact_deliveries", "forced", "critical", 1, 1, False)
    monkeypatch.setattr(quality, "run_silver_checks", lambda *args: [broken])

    with pytest.raises(quality.CriticalQualityError):
        run_tenant(spark, cfg, list(LAYER_ORDER), "run1")

    # Gold was never written, but the failed check was still logged.
    assert not Path(table_path(cfg, "gold", gold.TABLE)).exists()
    assert not Path(table_path(cfg, "gold", gold.TOP_TABLE)).exists()
    logged = spark.read.format("delta").load(cfg.paths.quality_logs)
    assert logged.filter("check_name = 'forced'").count() == 1
