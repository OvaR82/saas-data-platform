"""Refactor of mentoring/bad_code.py.

Same business intent as the original: for one tenant, keep the routine deliveries
(ZPRE, ZVE1), express the quantity in units (1 CS = 20 ST) and compute the revenue.

What changed and why is explained in mentoring/code_review.md. In short:
- Spark end to end (no pandas, no row-by-row loop) and an explicit schema.
- Business rules and paths come from the project configuration, not from literals.
- Invalid rows are rejected with a reason and counted, never published silently.
- Idempotent Delta write scoped to the processed date range.
- No side effects on import; reuses the tested helpers of the saas_pipeline package.

Usage (from the repository root):
    uv run python mentoring/good_code.py --tenant gt --start-date 2025-03-01 --end-date 2025-03-31
"""

from __future__ import annotations

import argparse
import logging
import sys

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from saas_pipeline.bronze import is_valid_process_date
from saas_pipeline.config import ENVIRONMENTS, load_config, to_partition_value
from saas_pipeline.session import get_spark
from saas_pipeline.silver import normalize_code, normalize_units
from saas_pipeline.storage import overwrite_date_range

log = logging.getLogger(__name__)

# Explicit contract of the source file: money and quantities as decimals, never floats.
DELIVERIES_SCHEMA = (
    "pais string, fecha_proceso string, transporte long, ruta long, tipo_entrega string, "
    "material string, precio decimal(38,18), cantidad decimal(38,18), unidad string"
)


def read_deliveries(spark: SparkSession, path: str) -> DataFrame:
    """Read the deliveries CSV with the explicit schema; fails if the header changes."""
    return (
        spark.read.option("header", True)
        .option("enforceSchema", False)
        .schema(DELIVERIES_SCHEMA)
        .csv(path)
    )


def routine_deliveries(
    df: DataFrame, tenant: str, routine_types: list[str], units_per_case: int
) -> tuple[DataFrame, DataFrame]:
    """Split one tenant's routine deliveries into (valid, rejected).

    valid: tenant_id, fecha_proceso, material, cantidad_normalizada_st, revenue.
    rejected: the original columns plus reject_reason.
    """
    # Tenant codes are lowercase by convention; normalize the source instead of trusting it.
    rows = df.withColumn("tenant_id", F.lower(F.trim("pais"))).filter(F.col("tenant_id") == tenant)
    # Routine deliveries only; exact duplicates are kept once.
    routine = rows.filter(normalize_code("tipo_entrega").isin(*routine_types)).dropDuplicates()
    # Shared, tested rule: CS -> ST, any other unit becomes null.
    with_units = normalize_units(routine, units_per_case)

    # First failing rule wins here: this script only needs to know that a row is unusable.
    reason = (
        F.when(~is_valid_process_date(F.col("fecha_proceso")), "invalid_fecha_proceso")
        .when(F.col("cantidad").isNull() | (F.col("cantidad") <= 0), "invalid_cantidad")
        .when(F.col("cantidad_normalizada_st").isNull(), "invalid_unidad")
        .when(F.col("precio").isNull(), "null_precio")
    )
    tagged = with_units.withColumn("reject_reason", reason)

    valid = tagged.filter(F.col("reject_reason").isNull()).select(
        "tenant_id",
        "fecha_proceso",
        "material",
        "cantidad_normalizada_st",
        (F.col("cantidad_normalizada_st") * F.col("precio")).alias("revenue"),
    )
    rejected = tagged.filter(F.col("reject_reason").isNotNull())
    return valid, rejected


def run(spark: SparkSession, cfg: DictConfig) -> dict[str, int]:
    """Process the configured tenant and date range; return the row counts."""
    tenant = cfg.tenant.code
    start = to_partition_value(cfg.execution.start_date)
    end = to_partition_value(cfg.execution.end_date)
    output_path = f"{cfg.paths.root}/mentoring/{tenant}/routine_deliveries"

    deliveries = read_deliveries(spark, f"{cfg.paths.raw}/{cfg.sources.deliveries}")
    valid, rejected = routine_deliveries(
        deliveries, tenant, cfg.business.routine_delivery_types, cfg.business.units_per_case
    )
    in_range = valid.filter(F.col("fecha_proceso").between(start, end))
    # Rejects of this run: undated rows (no range applies) plus dated rows inside the range.
    rejected = rejected.filter(
        ~is_valid_process_date(F.col("fecha_proceso")) | F.col("fecha_proceso").between(start, end)
    )

    # Delta + replaceWhere: re-running the same range replaces it, other dates are untouched.
    overwrite_date_range(in_range, output_path, start, end)

    # Rejected rows are not published, but they are visible: counted by reason in the log.
    for row in rejected.groupBy("reject_reason").count().collect():
        log.warning("[%s] rejected %s rows: %s", tenant, row["count"], row["reject_reason"])
    counts = {"written": in_range.count(), "rejected": rejected.count()}
    log.info("[%s] routine deliveries -> %s %s", tenant, output_path, counts)
    return counts


def main(argv: list[str] | None = None) -> int:
    """Command line entry point; returns a process exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env", choices=ENVIRONMENTS, default="dev")
    parser.add_argument("--tenant", required=True, help="Lowercase tenant code, e.g. gt")
    parser.add_argument("--start-date", help="First fecha_proceso, YYYY-MM-DD")
    parser.add_argument("--end-date", help="Last fecha_proceso, YYYY-MM-DD")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Dates are optional: when omitted, the range of the configuration is used.
    overrides = [
        f"execution.{key}={value}"
        for key, value in (("start_date", args.start_date), ("end_date", args.end_date))
        if value
    ]
    try:
        run(get_spark("mentoring-good-code"), load_config(args.env, args.tenant, overrides))
    except Exception:
        log.exception("Processing failed for tenant %s", args.tenant)
        return 1
    return 0


# Nothing runs on import: the module can be imported by tests and other code safely.
if __name__ == "__main__":
    sys.exit(main())
