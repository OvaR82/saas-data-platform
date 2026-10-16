"""Quality checks: SCD2 invariants, reconciliation, thresholds, persistence and the gate."""

from datetime import date
from decimal import Decimal

import pytest

from saas_pipeline import quality
from saas_pipeline.storage import TableRef

DIM_COLUMNS = "material string, valid_from date, valid_to date, is_current boolean"
# Counts that reconcile: 2 + 8 + 5 + 85 = 100.
METRICS = {"read": 100, "duplicates": 2, "discarded": 8, "quarantined": 5, "written": 85}


def test_scd2_checks_detect_overlaps_and_multiple_current_versions(spark):
    """SCD2 checks catch overlapping versions and several current versions."""
    dim = spark.createDataFrame(
        [
            ("A", date(2024, 1, 1), date(2025, 3, 31), False),
            ("A", date(2025, 3, 1), date(9999, 12, 31), True),  # overlaps the previous one
            ("B", date(2024, 1, 1), date(2024, 12, 31), True),
            ("B", date(2025, 1, 1), date(9999, 12, 31), True),  # second current version
        ],
        DIM_COLUMNS,
    )
    overlap = quality.check_no_overlapping_versions(dim)
    current = quality.check_single_current_version(dim)
    assert (overlap.records_failed, overlap.check_passed) == (1, False)
    assert (current.records_failed, current.records_checked, current.check_passed) == (1, 2, False)


def test_gap_check_detects_dates_without_a_version(spark):
    """A version that starts later than the day after the previous one ends leaves a gap."""
    dim = spark.createDataFrame(
        [
            ("A", date(2024, 1, 1), date(2024, 12, 31), False),
            ("A", date(2025, 1, 1), date(9999, 12, 31), True),  # contiguous: no gap
            ("B", date(2024, 1, 1), date(2024, 12, 31), False),
            ("B", date(2025, 2, 1), date(9999, 12, 31), True),  # January 2025 uncovered
        ],
        DIM_COLUMNS,
    )
    gaps = quality.check_no_version_gaps(dim)
    assert (gaps.records_failed, gaps.check_passed, gaps.severity) == (1, False, "warning")


def test_gold_check_detects_wrong_missing_and_extra_groups(spark):
    """Gold must match an independent aggregation of Silver, group by group."""
    fact = spark.createDataFrame(
        [
            ("20250301", "ZPRE", Decimal(10), Decimal(2)),
            ("20250301", "ZPRE", Decimal(5), Decimal(2)),  # ZPRE: 15 units, revenue 30
            ("20250301", "Z04", Decimal(1), Decimal(3)),  # Z04: 1 unit, revenue 3
        ],
        "fecha_proceso string, tipo_entrega string, cantidad_normalizada_st decimal(38,18), "
        "precio decimal(38,18)",
    )
    gold_columns = (
        "fecha_proceso string, tipo_entrega string, total_units decimal(38,18), "
        "total_revenue decimal(38,6)"
    )
    good = spark.createDataFrame(
        [
            ("20250301", "ZPRE", Decimal(15), Decimal(30)),
            ("20250301", "Z04", Decimal(1), Decimal(3)),
        ],
        gold_columns,
    )
    bad = spark.createDataFrame(
        [
            ("20250301", "ZPRE", Decimal(14), Decimal(30)),  # wrong units
            ("20250301", "Z05", Decimal(1), Decimal(1)),  # extra group; Z04 is missing
        ],
        gold_columns,
    )
    assert quality.check_gold_reconciles_with_silver(fact, good).check_passed
    result = quality.check_gold_reconciles_with_silver(fact, bad)
    assert (result.records_failed, result.layer, result.severity) == (3, "gold", "critical")


def test_reconciliation_detects_lost_rows():
    """Reconciliation fails when read rows are not fully accounted for."""
    assert quality.check_reconciliation(METRICS).check_passed
    # 5 rows unaccounted for.
    lost = quality.check_reconciliation({**METRICS, "written": 80})
    assert (lost.records_failed, lost.check_passed, lost.severity) == (5, False, "critical")


def test_quarantine_rate_threshold():
    """The quarantine-rate warning compares against the configured threshold."""
    # 5 quarantined out of 100 read = 5%.
    assert quality.check_quarantine_rate(METRICS, max_rate=0.05).check_passed
    assert not quality.check_quarantine_rate(METRICS, max_rate=0.04).check_passed


def test_quality_logs_follow_the_architecture_schema(spark, tmp_path):
    """quality_logs is written with the exact schema of architecture 5.9."""
    path = str(tmp_path / "quality_logs")
    results = [
        quality.check_reconciliation(METRICS),
        quality.check_discarded_delivery_types(METRICS),
    ]
    quality.write_quality_logs(spark, results, TableRef(path=path), "run1", "run1_sv", "sv")

    # Exact column names and order of architecture 5.9.
    logs = spark.read.format("delta").load(path)
    assert logs.columns == [
        "_run_id",
        "_batch_id",
        "tenant_id",
        "layer",
        "table_name",
        "check_name",
        "check_severity",
        "records_checked",
        "records_failed",
        "check_passed",
        "executed_at",
    ]
    assert logs.count() == 2


def test_enforce_raises_only_when_configured():
    """A critical failure only aborts when fail_on_critical is true."""
    failed = quality.check_reconciliation({**METRICS, "written": 0})
    quality.enforce([failed], "sv", fail_on_critical=False)  # only logs a warning
    with pytest.raises(quality.CriticalQualityError):
        quality.enforce([failed], "sv", fail_on_critical=True)
