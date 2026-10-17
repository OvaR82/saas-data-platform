"""Silver (and Gold) pure transformations, tested on small in-memory DataFrames."""

from datetime import date
from decimal import Decimal

import pytest

from saas_pipeline import gold, silver

# Schemas of the in-memory test DataFrames (same column names as the real tables).
DIM_COLUMNS = (
    "material string, descripcion string, categoria string, precio_base decimal(38,18), "
    "valid_from date, valid_to date, is_current boolean"
)
FACT_COLUMNS = (
    "tenant_id string, fecha_proceso string, fecha_proceso_date date, transporte long, "
    "ruta long, tipo_entrega string, material string, precio decimal(38,18), "
    "cantidad decimal(38,18), unidad string"
)


@pytest.fixture
def dim(spark):
    """DA030001 changes price on 2025-02-01 and 2025-05-16 (same shape as the real catalog)."""
    versions = [
        (Decimal("55"), date(2024, 1, 1), date(2025, 1, 31)),
        (Decimal("57.5"), date(2025, 2, 1), date(2025, 5, 15)),
        (Decimal("60"), date(2025, 5, 16), date(9999, 12, 31)),
    ]
    rows = [
        ("DA030001", "Energizante", "ENERGETICOS", price, start, end, end.year == 9999)
        for price, start, end in versions
    ]
    return spark.createDataFrame(rows, DIM_COLUMNS)


def fact_row(
    process_date="20250314",
    delivery_type="ZPRE",
    material="DA030001",
    price="10",
    quantity="2",
    unit="CS",
    transport=1,
    route=1,
):
    """Return one typed fact row; each test overrides only the fields it cares about."""

    def to_decimal(value):
        """Convert a string to Decimal, keeping None as null."""
        return None if value is None else Decimal(value)

    day = date(int(process_date[:4]), int(process_date[4:6]), int(process_date[6:]))
    return (
        "sv",
        process_date,
        day,
        transport,
        route,
        delivery_type,
        material,
        to_decimal(price),
        to_decimal(quantity),
        unit,
    )


def test_cast_types_turns_garbage_into_nulls_under_ansi(spark):
    """Unparseable Bronze strings become nulls (later quarantined) instead of failing the job.

    The test session runs with ANSI mode on, where a plain cast would raise an error.
    """
    columns = (
        "_tenant_id string, fecha_proceso string, transporte string, ruta string, "
        "tipo_entrega string, material string, precio string, cantidad string, unidad string, "
        "_source_file string, _ingestion_timestamp timestamp, _batch_id string"
    )
    rows = [
        ("sv", "20250314", "x12", "", " zpre ", "AA004003", "abc", "1,5", "cs", "f", None, "b")
    ]
    out = silver.cast_types(spark.createDataFrame(rows, columns)).first()

    assert (out.transporte, out.ruta, out.precio, out.cantidad) == (None, None, None, None)
    assert out.fecha_proceso_date == date(2025, 3, 14)
    assert (out.unidad, out.tipo_entrega) == ("CS", "ZPRE")  # codes trimmed and uppercased


def test_normalize_units_converts_cases_to_units(spark):
    """CS quantities are multiplied by 20; ST quantities are unchanged."""
    # 2 CS -> 40 ST; 2 ST stay 2.
    df = spark.createDataFrame([fact_row(unit="CS"), fact_row(unit="ST")], FACT_COLUMNS)
    out = silver.normalize_units(df, units_per_case=20).collect()
    assert [r.cantidad_normalizada_st for r in out] == [Decimal(40), Decimal(2)]


def test_delivery_flags(spark):
    """Routine (ZPRE, ZVE1) and bonus (Z04, Z05) flags are set from the type."""
    rows = [fact_row(delivery_type=t) for t in ("ZPRE", "ZVE1", "Z04", "Z05")]
    df = spark.createDataFrame(rows, FACT_COLUMNS)
    out = silver.add_delivery_flags(df, ["ZPRE", "ZVE1"], ["Z04", "Z05"]).collect()
    flags = [(r.is_routine_delivery, r.is_bonus_delivery) for r in out]
    assert flags == [(True, False), (True, False), (False, True), (False, True)]


def test_temporal_join_uses_version_valid_on_process_date(spark, dim):
    """Joining on is_current would price March at 60; the version valid in March is 57.5."""
    # One delivery inside each of the three versions of DA030001.
    rows = [
        fact_row(process_date="20250115"),
        fact_row(process_date="20250314"),
        fact_row(process_date="20250620"),
    ]
    df = spark.createDataFrame(rows, FACT_COLUMNS)
    out = silver.enrich_with_materials(df, dim).orderBy("fecha_proceso").collect()
    assert [r.precio_base for r in out] == [Decimal(55), Decimal("57.5"), Decimal(60)]
    assert all(r._material_found for r in out)


def test_split_anomalies_quarantines_with_every_reason(spark, dim):
    """Each anomaly goes to quarantine with every reason it matches."""
    rows = [
        fact_row(),  # valid
        fact_row(quantity="0", route=2),
        fact_row(quantity="-5", route=3),
        fact_row(quantity=None, route=4),
        fact_row(price=None, route=5),
        fact_row(material="XX999999", route=6),
        fact_row(material="XX999998", quantity="0", route=7),  # two problems at once
        fact_row(unit="KG", route=8),
        fact_row(process_date="20231215", route=9),  # known SKU, but before its first version
    ]
    # `ruta` identifies each case in the assertions below.
    df = silver.enrich_with_materials(spark.createDataFrame(rows, FACT_COLUMNS), dim)
    valid, quarantine = silver.split_anomalies(df)

    assert [r.ruta for r in valid.collect()] == [1]
    reasons = {r.ruta: r._quarantine_reason for r in quarantine.collect()}
    assert reasons == {
        2: "non_positive_cantidad",
        3: "non_positive_cantidad",
        4: "null_cantidad",
        5: "null_precio",
        6: "material_not_in_catalog",
        7: "non_positive_cantidad;material_not_in_catalog",
        8: "invalid_unidad",
        9: "no_material_version_at_date",
    }


def test_dim_is_current_is_derived_from_valid_to(spark, dim):
    """is_current is recomputed from valid_to, ignoring the source flag."""
    # Invert the source flags: the result must still follow valid_to.
    wrong_flags = dim.withColumn("is_current", ~dim.is_current)
    out = silver.prepare_dim_materials(wrong_flags).orderBy("valid_from").collect()
    assert [r.is_current for r in out] == [False, False, True]


def test_gold_metrics_use_normalized_units_and_transaction_price(spark):
    """Gold sums normalized units and revenue at the transaction price."""
    rows = [
        fact_row(price="1.5", quantity="2", unit="CS", transport=10),  # 40 ST
        fact_row(price="2", quantity="5", unit="ST", transport=11),  # 5 ST
        fact_row(delivery_type="Z04", price="3", quantity="1", unit="ST", route=2),
    ]
    # Gold reads Silver, where cantidad_normalizada_st already exists.
    df = silver.normalize_units(spark.createDataFrame(rows, FACT_COLUMNS), 20)
    out = {r.tipo_entrega: r for r in gold.build_daily_metrics(df).collect()}

    assert out["ZPRE"].total_units == Decimal(45)
    assert out["ZPRE"].total_revenue == Decimal(40 * 1.5 + 5 * 2)
    assert (out["ZPRE"].active_routes, out["ZPRE"].active_transports) == (1, 2)
    assert out["Z04"].total_units == Decimal(1)
