"""Silver: materials catalog as SCD Type 2 (dim) and clean, enriched deliveries (fact).

Flow for fact_deliveries (counts reconcile: read = duplicates + discarded + quarantined + written):
    bronze range -> drop exact duplicates -> discard out-of-scope tipo_entrega
    -> type columns -> normalize units -> flags -> temporal join with dim_materials
    -> split anomalies (quarantine) -> MERGE on the business key
"""

from __future__ import annotations

import logging
from datetime import date

from omegaconf import DictConfig
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from saas_pipeline import bronze
from saas_pipeline.config import quarantine_path, table_path, to_partition_value
from saas_pipeline.storage import date_range_predicate, merge_into, overwrite_date_range

log = logging.getLogger(__name__)

FACT_TABLE = "fact_deliveries"
DIM_TABLE = "dim_materials"
# Same precision as the source (18 decimals): money and quantities are never floats.
DECIMAL = DecimalType(38, 18)
# valid_to of the version that is still in force.
OPEN_END = date(9999, 12, 31)
VALID_UNITS = ("CS", "ST")
# Business keys used by the MERGE statements (architecture 5.5).
FACT_KEY = ("tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega")
DIM_KEY = ("material", "valid_from")
# Explicit schema with enforceSchema=False: a renamed or reordered column fails the read.
CATALOG_SCHEMA = (
    "material string, descripcion string, categoria string, precio_base decimal(38,18), "
    "valid_from date, valid_to date, is_current boolean"
)


# --- dim_materials (SCD Type 2) --------------------------------------------------------------


def read_catalog(spark: SparkSession, path: str) -> DataFrame:
    """Read the materials catalog CSV already typed (dates, decimal price, boolean flag)."""
    return (
        spark.read.option("header", True)
        .option("enforceSchema", False)
        .schema(CATALOG_SCHEMA)
        .csv(path)
    )


def prepare_dim_materials(catalog: DataFrame) -> DataFrame:
    """Prepare the catalog versions to merge into dim_materials.

    Keeps one row per version (material, valid_from) and derives is_current from
    valid_to: the flag shipped in the CSV is informative, not the source of truth.
    """
    return catalog.dropDuplicates(list(DIM_KEY)).withColumn(
        "is_current", F.col("valid_to") == F.lit(OPEN_END)
    )


# --- fact_deliveries -------------------------------------------------------------------------


def cast_types(df: DataFrame) -> DataFrame:
    """Convert Bronze strings to business types and keep the lineage columns.

    fecha_proceso stays as yyyymmdd (partition key); fecha_proceso_date is added as date.
    """
    return df.select(
        # Business columns. A value that cannot be cast becomes null and the quarantine
        # rules catch it (null_cantidad, null_precio, null_business_key).
        F.col("_tenant_id").alias("tenant_id"),
        "fecha_proceso",
        F.to_date("fecha_proceso", "yyyyMMdd").alias("fecha_proceso_date"),
        F.col("transporte").cast("long").alias("transporte"),
        F.col("ruta").cast("long").alias("ruta"),
        F.trim("tipo_entrega").alias("tipo_entrega"),
        F.trim("material").alias("material"),
        F.col("precio").cast(DECIMAL).alias("precio"),
        F.col("cantidad").cast(DECIMAL).alias("cantidad"),
        F.upper(F.trim("unidad")).alias("unidad"),
        # Technical columns carried over from Bronze for lineage.
        "_source_file",
        "_ingestion_timestamp",
        "_tenant_id",
    )


def normalize_units(df: DataFrame, units_per_case: int) -> DataFrame:
    """Add cantidad_normalizada_st: the quantity in ST (1 CS = units_per_case ST)."""
    # Any other unit yields null and is quarantined as invalid_unidad.
    qty = F.when(F.col("unidad") == "CS", F.col("cantidad") * units_per_case).when(
        F.col("unidad") == "ST", F.col("cantidad")
    )
    return df.withColumn("cantidad_normalizada_st", qty.cast(DECIMAL))


def add_delivery_flags(df: DataFrame, routine: list[str], bonus: list[str]) -> DataFrame:
    """is_routine_delivery / is_bonus_delivery from the delivery types listed in config."""
    return df.withColumn("is_routine_delivery", F.col("tipo_entrega").isin(*routine)).withColumn(
        "is_bonus_delivery", F.col("tipo_entrega").isin(*bonus)
    )


def enrich_with_materials(fact: DataFrame, dim: DataFrame) -> DataFrame:
    """Add the catalog attributes valid on fecha_proceso (temporal join, not is_current).

    Also adds _material_found, used by the quarantine rules and dropped afterwards.
    """
    # Rename the dim key to avoid an ambiguous `material` column after the join.
    versions = dim.select(
        F.col("material").alias("_dim_material"),
        "descripcion",
        "categoria",
        "precio_base",
        "valid_from",
        "valid_to",
    )
    # Same SKU and fecha_proceso inside [valid_from, valid_to] (both ends inclusive).
    on = (F.col("material") == F.col("_dim_material")) & F.col("fecha_proceso_date").between(
        F.col("valid_from"), F.col("valid_to")
    )
    # Left join: a row without a matching version is kept (and flagged) so it can be
    # quarantined instead of disappearing silently.
    return (
        fact.join(versions, on, "left")
        .withColumn("_material_found", F.col("_dim_material").isNotNull())
        .drop("_dim_material", "valid_from", "valid_to")
    )


def _flag(condition: Column) -> Column:
    """Make a rule null-safe: a comparison against null counts as 'not matched'."""
    return F.coalesce(condition, F.lit(False))


def quarantine_rules() -> list[tuple[str, Column]]:
    """Quarantine policy (architecture 5.6) as (reason, condition) pairs."""
    return [
        (
            "null_business_key",
            F.col("transporte").isNull() | F.col("ruta").isNull() | F.col("material").isNull(),
        ),
        ("null_cantidad", F.col("cantidad").isNull()),
        ("non_positive_cantidad", F.col("cantidad") <= 0),
        ("null_precio", F.col("precio").isNull()),
        ("invalid_unidad", F.col("unidad").isNull() | ~F.col("unidad").isin(*VALID_UNITS)),
        ("material_not_in_catalog", ~F.col("_material_found")),
    ]


def split_anomalies(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows into (valid, quarantined) by applying every quarantine rule.

    Every matching reason is kept in _quarantine_reason, separated by ';', so a
    problem is never hidden behind another one.
    """
    # One entry per matching rule (null when it does not match), nulls removed afterwards.
    reasons = F.array_compact(
        F.array(*[F.when(_flag(rule), F.lit(name)) for name, rule in quarantine_rules()])
    )
    tagged = df.withColumn("_quarantine_reason", F.concat_ws(";", reasons)).drop("_material_found")
    # An empty reason means the row passed every rule.
    valid = tagged.filter(F.col("_quarantine_reason") == "").drop("_quarantine_reason")
    quarantine = tagged.filter(F.col("_quarantine_reason") != "")
    return valid, quarantine


def build_fact(deliveries: DataFrame, dim: DataFrame, cfg: DictConfig) -> DataFrame:
    """Chain the Silver transformations: types, units, flags and temporal enrichment.

    Returns every in-scope row; the anomaly split happens afterwards.
    """
    business = cfg.business
    df = cast_types(deliveries)
    df = normalize_units(df, business.units_per_case)
    df = add_delivery_flags(df, business.routine_delivery_types, business.bonus_delivery_types)
    return enrich_with_materials(df, dim)


# --- layer entry point -----------------------------------------------------------------------


def run(spark: SparkSession, cfg: DictConfig, batch_id: str) -> dict[str, int]:
    """Build dim_materials and fact_deliveries for one tenant; return the reconciliation counts."""
    tenant = cfg.tenant.code
    start = to_partition_value(cfg.execution.start_date)
    end = to_partition_value(cfg.execution.end_date)
    dim_path = table_path(cfg, "silver", DIM_TABLE)
    fact_path = table_path(cfg, "silver", FACT_TABLE)

    # dim_materials: MERGE on (material, valid_from); history is never deleted.
    catalog = prepare_dim_materials(read_catalog(spark, f"{cfg.paths.raw}/{cfg.sources.materials}"))
    merge_into(
        spark,
        catalog.withColumn("_batch_id", F.lit(batch_id)).withColumn(
            "_updated_at", F.current_timestamp()
        ),
        dim_path,
        DIM_KEY,
    )
    dim = spark.read.format("delta").load(dim_path)

    # fact_deliveries: Silver only reads Bronze (never RAW), for the processed range.
    deliveries = (
        spark.read.format("delta")
        .load(table_path(cfg, "bronze", bronze.TABLE))
        .filter(F.col("fecha_proceso").between(start, end))
    )
    # Exact duplicates (all source columns equal) keep a single copy; not an anomaly (5.6).
    deduplicated = deliveries.dropDuplicates(list(bronze.SOURCE_COLUMNS))
    # Out-of-scope delivery types (COBR, Z99) are discarded: counted, never persisted.
    valid_types = [*cfg.business.routine_delivery_types, *cfg.business.bonus_delivery_types]
    in_scope = deduplicated.filter(F.trim("tipo_entrega").isin(*valid_types))

    # Cached: the join feeds the MERGE, the quarantine write and the counts.
    fact = build_fact(in_scope, dim, cfg).withColumn("_batch_id", F.lit(batch_id)).cache()
    valid, quarantine = split_anomalies(fact)

    # Valid rows: MERGE on the business key, scoped to the processed range.
    merge_into(
        spark,
        valid,
        fact_path,
        FACT_KEY,
        partition_by="fecha_proceso",
        scope=date_range_predicate(start, end, alias="t"),
    )
    # Quarantined rows: replaced for the same range, so reprocessing never duplicates them.
    overwrite_date_range(quarantine, quarantine_path(cfg, "silver", FACT_TABLE), start, end)

    # Reconciliation counts: read = duplicates + discarded + quarantined + written.
    read, unique, kept = deliveries.count(), deduplicated.count(), in_scope.count()
    metrics = {
        "read": read,
        "duplicates": read - unique,
        "discarded": unique - kept,
        "quarantined": quarantine.count(),
        "written": valid.count(),
    }
    fact.unpersist()
    log.info("[%s] silver.%s %s", tenant, FACT_TABLE, metrics)
    return metrics
