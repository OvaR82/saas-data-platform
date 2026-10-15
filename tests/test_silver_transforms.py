from datetime import date
from decimal import Decimal

import pytest

from saas_pipeline import gold, silver

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
    fecha="20250314",
    tipo="ZPRE",
    material="DA030001",
    precio="10",
    cantidad="2",
    unidad="CS",
    transporte=1,
    ruta=1,
):
    def to_decimal(value):
        return None if value is None else Decimal(value)

    process_date = date(int(fecha[:4]), int(fecha[4:6]), int(fecha[6:]))
    return (
        "sv",
        fecha,
        process_date,
        transporte,
        ruta,
        tipo,
        material,
        to_decimal(precio),
        to_decimal(cantidad),
        unidad,
    )


def test_normalize_units_converts_cases_to_units(spark):
    df = spark.createDataFrame([fact_row(unidad="CS"), fact_row(unidad="ST")], FACT_COLUMNS)
    out = silver.normalize_units(df, units_per_case=20).collect()
    assert [r.cantidad_normalizada_st for r in out] == [Decimal(40), Decimal(2)]


def test_delivery_flags(spark):
    rows = [fact_row(tipo=t) for t in ("ZPRE", "ZVE1", "Z04", "Z05")]
    df = spark.createDataFrame(rows, FACT_COLUMNS)
    out = silver.add_delivery_flags(df, ["ZPRE", "ZVE1"], ["Z04", "Z05"]).collect()
    flags = [(r.is_routine_delivery, r.is_bonus_delivery) for r in out]
    assert flags == [(True, False), (True, False), (False, True), (False, True)]


def test_temporal_join_uses_version_valid_on_process_date(spark, dim):
    """Joining on is_current would price March at 60; the version valid in March is 57.5."""
    rows = [fact_row(fecha="20250115"), fact_row(fecha="20250314"), fact_row(fecha="20250620")]
    df = spark.createDataFrame(rows, FACT_COLUMNS)
    out = silver.enrich_with_materials(df, dim).orderBy("fecha_proceso").collect()
    assert [r.precio_base for r in out] == [Decimal(55), Decimal("57.5"), Decimal(60)]
    assert all(r._material_found for r in out)


def test_split_anomalies_quarantines_with_every_reason(spark, dim):
    rows = [
        fact_row(),  # valid
        fact_row(cantidad="0", ruta=2),
        fact_row(cantidad="-5", ruta=3),
        fact_row(cantidad=None, ruta=4),
        fact_row(precio=None, ruta=5),
        fact_row(material="XX999999", ruta=6),
        fact_row(material="XX999998", cantidad="0", ruta=7),  # two problems at once
        fact_row(unidad="KG", ruta=8),
    ]
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
    }


def test_dim_is_current_is_derived_from_valid_to(spark, dim):
    wrong_flags = dim.withColumn("is_current", ~dim.is_current)
    out = silver.prepare_dim_materials(wrong_flags).orderBy("valid_from").collect()
    assert [r.is_current for r in out] == [False, False, True]


def test_gold_metrics_use_normalized_units_and_transaction_price(spark):
    rows = [
        fact_row(precio="1.5", cantidad="2", unidad="CS", transporte=10),  # 40 ST
        fact_row(precio="2", cantidad="5", unidad="ST", transporte=11),  # 5 ST
        fact_row(tipo="Z04", precio="3", cantidad="1", unidad="ST", ruta=2),
    ]
    df = silver.normalize_units(spark.createDataFrame(rows, FACT_COLUMNS), 20)
    out = {r.tipo_entrega: r for r in gold.build_daily_metrics(df).collect()}

    assert out["ZPRE"].total_units == Decimal(45)
    assert out["ZPRE"].total_revenue == Decimal(40 * 1.5 + 5 * 2)
    assert (out["ZPRE"].active_routes, out["ZPRE"].active_transports) == (1, 2)
    assert out["Z04"].total_units == Decimal(1)
