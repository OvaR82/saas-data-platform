"""Refactor of bad_code.py, written to be read next to the original.

Same business rule: for one tenant, keep the routine deliveries (ZPRE, ZVE1), express the
quantity in units (1 CS = 20 ST) and compute the total. What changed, and why:
- Spark end to end, by columns: no pandas, no row loop. It scales and runs where the data is.
- Explicit schema with decimals: money is never a float, and a changed header fails loudly.
- Invalid rows are rejected with a reason and counted, never published silently.
- Atomic Delta overwrite of the tenant's table: running it twice gives the same result, and a
  crash halfway leaves the previous version intact.
- Paths and tenant are arguments, nothing runs on import, errors are logged with an exit code.
In the pipeline (src/saas_pipeline) the constants below come from the configuration and the unit
conversion is a shared, tested function; here they stay inline so the script is self-contained.

Usage:
    python mentoring/good_code.py --input data/raw/global_mobility_data_entrega_productos.csv \\
        --output data/mentoring/routine_deliveries --tenant gt
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

SCHEMA = (
    "pais string, fecha_proceso string, transporte long, ruta long, tipo_entrega string, "
    "material string, precio decimal(38,18), cantidad decimal(38,18), unidad string"
)
ROUTINE_TYPES = ("ZPRE", "ZVE1")
UNITS_PER_CASE = 20


def routine_deliveries(df: DataFrame, tenant: str) -> tuple[DataFrame, DataFrame]:
    """Split one tenant's routine deliveries into (valid rows, rows rejected with a reason)."""
    rows = (
        df.filter(F.lower(F.trim("pais")) == tenant)  # tenant codes are lowercase
        .filter(F.upper(F.trim("tipo_entrega")).isin(*ROUTINE_TYPES))
        .dropDuplicates()
    )
    unit = F.upper(F.trim("unidad"))
    # CS -> ST; any other unit gives null and is rejected (bad_code.py counted it as ST).
    units = F.when(unit == "CS", F.col("cantidad") * UNITS_PER_CASE).when(
        unit == "ST", F.col("cantidad")
    )
    reason = (
        F.when(F.try_to_timestamp("fecha_proceso", F.lit("yyyyMMdd")).isNull(), "invalid_fecha")
        .when(F.col("cantidad").isNull() | (F.col("cantidad") <= 0), "invalid_cantidad")
        .when(units.isNull(), "invalid_unidad")
        .when(F.col("precio").isNull(), "null_precio")
    )
    tagged = rows.withColumn("cantidad_st", units).withColumn("reject_reason", reason)
    valid = tagged.filter(F.col("reject_reason").isNull()).select(
        "pais",
        "fecha_proceso",
        "material",
        "cantidad_st",
        (F.col("cantidad_st") * F.col("precio")).alias("total"),
    )
    return valid, tagged.filter(F.col("reject_reason").isNotNull())


def process(spark: SparkSession, input_path: str, output_path: str, tenant: str) -> dict[str, int]:
    """Read the CSV, keep the tenant's routine deliveries and write them as a Delta table."""
    source = (
        spark.read.option("header", True)
        .option("enforceSchema", False)
        .schema(SCHEMA)
        .csv(input_path)
    )
    valid, rejected = routine_deliveries(source, tenant)
    valid.write.format("delta").mode("overwrite").save(f"{output_path}/{tenant}")
    for row in rejected.groupBy("reject_reason").count().collect():
        log.warning("[%s] rejected %s rows: %s", tenant, row["count"], row["reject_reason"])
    counts = {"written": valid.count(), "rejected": rejected.count()}
    log.info("[%s] routine deliveries -> %s/%s %s", tenant, output_path, tenant, counts)
    return counts


def get_spark() -> SparkSession:
    """The cluster's session on Databricks; locally, one with Delta Lake enabled."""
    if spark := SparkSession.getActiveSession():
        return spark
    from delta import configure_spark_with_delta_pip

    builder = SparkSession.builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    ).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main(argv: list[str] | None = None) -> int:
    """Command line entry point; returns a process exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", required=True, help="Deliveries CSV")
    parser.add_argument("--output", required=True, help="Base folder; the tenant is appended")
    parser.add_argument("--tenant", required=True, help="Tenant code, e.g. gt")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        process(get_spark(), args.input, args.output, args.tenant.lower())
    except Exception:
        log.exception("Processing failed for tenant %s", args.tenant)
        return 1
    return 0


if __name__ == "__main__":  # nothing runs on import: tests and other code can reuse the functions
    sys.exit(main())
