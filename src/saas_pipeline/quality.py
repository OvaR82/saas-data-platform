"""Data quality checks persisted to the shared quality_logs Delta table (architecture 5.9).

Each check returns a CheckResult; severity drives the reaction:
critical -> may abort before Gold (quality.fail_on_critical), warning -> logged, info -> metric.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from saas_pipeline.silver import FACT_KEY

log = logging.getLogger(__name__)

# Severity levels allowed in quality_logs.check_severity.
CRITICAL, WARNING, INFO = "critical", "warning", "info"

# Schema of quality_logs (architecture 5.9); executed_at is added when writing.
QUALITY_LOG_COLUMNS = (
    "_run_id string, _batch_id string, tenant_id string, layer string, table_name string, "
    "check_name string, check_severity string, records_checked long, records_failed long, "
    "check_passed boolean"
)


class CriticalQualityError(RuntimeError):
    """A critical check failed and quality.fail_on_critical is enabled."""


@dataclass(frozen=True)
class CheckResult:
    """Outcome of one check; maps 1:1 to a row of quality_logs."""

    layer: str
    table_name: str
    check_name: str
    severity: str
    records_checked: int
    records_failed: int
    check_passed: bool


def _result(layer, table, name, severity, checked, failed, passed=None) -> CheckResult:
    """Build a CheckResult; by default a check passes when no record failed."""
    passed = failed == 0 if passed is None else passed
    return CheckResult(layer, table, name, severity, int(checked), int(failed), passed)


# --- Silver checks -----------------------------------------------------------------------


def check_unique_business_key(fact: DataFrame, keys: tuple[str, ...]) -> CheckResult:
    """MERGE relies on the key being unique; duplicates would make it ambiguous."""
    duplicated = fact.groupBy(*keys).count().filter("count > 1")
    # Rows involved in a duplicated key (0 when there is none).
    failed = duplicated.agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0]
    return _result(
        "silver", "fact_deliveries", "unique_business_key", CRITICAL, fact.count(), failed
    )


def check_material_enriched(fact: DataFrame) -> CheckResult:
    """Every fact row must have found its catalog version in the temporal join."""
    # categoria comes from the dimension, so null means the join found no version.
    failed = fact.filter(F.col("categoria").isNull()).count()
    return _result(
        "silver", "fact_deliveries", "material_version_enriched", CRITICAL, fact.count(), failed
    )


def check_single_current_version(dim: DataFrame) -> CheckResult:
    """SCD2 invariant: exactly one is_current = true per material."""
    # Number of current versions per SKU; checked per material, not per row.
    per_material = dim.groupBy("material").agg(F.sum(F.col("is_current").cast("int")).alias("n"))
    failed = per_material.filter("n != 1").count()
    return _result(
        "silver", "dim_materials", "single_current_version", CRITICAL, per_material.count(), failed
    )


def check_no_overlapping_versions(dim: DataFrame) -> CheckResult:
    """SCD2 invariant: versions of a material must not overlap, otherwise the temporal join
    would duplicate fact rows."""
    # Compare each version with the previous one of the same SKU: it must start after the
    # previous one ends. (Spark does not allow window functions directly inside a filter.)
    previous_end = F.lag("valid_to").over(Window.partitionBy("material").orderBy("valid_from"))
    with_previous = dim.withColumn("_previous_end", previous_end)
    failed = with_previous.filter(F.col("_previous_end") >= F.col("valid_from")).count()
    return _result(
        "silver", "dim_materials", "no_overlapping_versions", CRITICAL, dim.count(), failed
    )


def check_reconciliation(metrics: dict[str, int]) -> CheckResult:
    """Nothing is lost silently: read = duplicates + discarded + quarantined + written."""
    accounted = sum(metrics[k] for k in ("duplicates", "discarded", "quarantined", "written"))
    failed = abs(metrics["read"] - accounted)
    return _result(
        "silver", "fact_deliveries", "row_reconciliation", CRITICAL, metrics["read"], failed
    )


def check_quarantine_rate(metrics: dict[str, int], max_rate: float) -> CheckResult:
    """A spike in quarantined rows usually means a broken source, not bad luck."""
    read, quarantined = metrics["read"], metrics["quarantined"]
    # An empty range is not a failure; otherwise compare against the configured threshold.
    passed = read == 0 or quarantined / read <= max_rate
    return _result(
        "silver", "fact_deliveries", "quarantine_rate", WARNING, read, quarantined, passed
    )


def check_discarded_delivery_types(metrics: dict[str, int]) -> CheckResult:
    """Discards are a business rule (COBR, Z99): counted, never persisted."""
    return _result(
        "silver",
        "fact_deliveries",
        "discarded_delivery_types",
        INFO,
        metrics["read"],
        metrics["discarded"],
        passed=True,
    )


def check_bronze_process_date(metrics: dict[str, int]) -> CheckResult:
    """Rows without a valid fecha_proceso were sent to bronze_quarantine."""
    return _result(
        "bronze",
        "deliveries",
        "valid_fecha_proceso",
        WARNING,
        metrics["written"] + metrics["quarantined"],
        metrics["quarantined"],
    )


def run_silver_checks(
    fact: DataFrame, dim: DataFrame, metrics: dict[str, int], max_quarantine_rate: float
) -> list[CheckResult]:
    """Every Silver check, in the order they appear in quality_logs."""
    return [
        check_unique_business_key(fact, FACT_KEY),
        check_material_enriched(fact),
        check_single_current_version(dim),
        check_no_overlapping_versions(dim),
        check_reconciliation(metrics),
        check_quarantine_rate(metrics, max_quarantine_rate),
        check_discarded_delivery_types(metrics),
    ]


# --- persistence and enforcement -----------------------------------------------------------


def write_quality_logs(
    spark: SparkSession,
    results: list[CheckResult],
    path: str,
    run_id: str,
    batch_id: str,
    tenant: str,
) -> None:
    """Append-only audit trail: each run adds its own rows (identified by _run_id)."""
    if not results:
        return
    # One row per check, in the column order of QUALITY_LOG_COLUMNS.
    rows = [
        (
            run_id,
            batch_id,
            tenant,
            r.layer,
            r.table_name,
            r.check_name,
            r.severity,
            r.records_checked,
            r.records_failed,
            r.check_passed,
        )
        for r in results
    ]
    logs = spark.createDataFrame(rows, QUALITY_LOG_COLUMNS).withColumn(
        "executed_at", F.current_timestamp()
    )
    logs.write.format("delta").mode("append").save(path)


def enforce(results: list[CheckResult], tenant: str, fail_on_critical: bool) -> None:
    """Log every failed check and, if configured, stop the tenant before Gold."""
    # Failed checks of any severity are visible in the run log, not only in quality_logs.
    for r in results:
        if not r.check_passed:
            log.warning(
                "[%s] %s check failed: %s.%s (%s/%s)",
                tenant,
                r.severity,
                r.table_name,
                r.check_name,
                r.records_failed,
                r.records_checked,
            )
    # Only critical failures can stop the run, and only when fail_on_critical is enabled.
    critical = [r.check_name for r in results if r.severity == CRITICAL and not r.check_passed]
    if critical and fail_on_critical:
        raise CriticalQualityError(f"[{tenant}] critical checks failed: {critical}")
