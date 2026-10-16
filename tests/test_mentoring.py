"""mentoring/good_code.py: same business result as bad_code.py, without its silent errors."""

import importlib.util
from decimal import Decimal
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
COLUMNS = (
    "pais string, fecha_proceso string, transporte long, ruta long, tipo_entrega string, "
    "material string, precio decimal(38,18), cantidad decimal(38,18), unidad string"
)


@pytest.fixture(scope="module")
def good_code():
    """Import mentoring/good_code.py by path (mentoring/ is not a package)."""
    spec = importlib.util.spec_from_file_location(
        "good_code", REPO_ROOT / "mentoring/good_code.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def row(pais="GT", fecha="20250314", tipo="ZPRE", precio="1.5", cantidad="2", unidad="CS", ruta=1):
    """Return one source row; each test overrides only the fields it cares about."""
    return (pais, fecha, 1, ruta, tipo, "AA004003", Decimal(precio) if precio else None,
            Decimal(cantidad) if cantidad else None, unidad)  # fmt: skip


def test_keeps_the_original_business_rules(spark, good_code):
    """Routine only, CS x 20, revenue = units x price, and 'GT'/'gt' are the same tenant."""
    rows = [
        row(unidad="CS", ruta=1),  # 2 CS -> 40 ST, revenue 60
        row(pais="gt", unidad="ST", cantidad="5", ruta=2),  # lowercase source still matches
        row(tipo="Z04", ruta=3),  # bonus: not part of this report
        row(pais="SV", ruta=4),  # another tenant
    ]
    df = spark.createDataFrame(rows, COLUMNS)
    valid, rejected = good_code.routine_deliveries(df, "gt", ["ZPRE", "ZVE1"], 20)

    out = {r.cantidad_normalizada_st: r.revenue for r in valid.collect()}
    assert out == {Decimal(40): Decimal(60), Decimal(5): Decimal("7.5")}
    assert rejected.count() == 0


def test_rejects_what_the_original_published_silently(spark, good_code):
    """Rows that bad_code.py would have published with wrong values are rejected with a reason."""
    rows = [
        row(cantidad="-3", ruta=1),
        row(cantidad=None, ruta=2),
        row(unidad="KG", ruta=3),  # bad_code.py counted any non-CS unit as ST
        row(precio=None, ruta=4),
        row(fecha=None, ruta=5),
        row(fecha="2025-03-15", ruta=6),
        row(ruta=7),
        row(ruta=7),  # exact duplicate: kept once
    ]
    df = spark.createDataFrame(rows, COLUMNS)
    valid, rejected = good_code.routine_deliveries(df, "gt", ["ZPRE", "ZVE1"], 20)

    assert [r.cantidad_normalizada_st for r in valid.collect()] == [Decimal(40)]
    reasons = {r.ruta: r.reject_reason for r in rejected.collect()}
    assert reasons == {
        1: "invalid_cantidad",
        2: "invalid_cantidad",
        3: "invalid_unidad",
        4: "null_precio",
        5: "invalid_fecha_proceso",
        6: "invalid_fecha_proceso",
    }


def test_run_is_idempotent_and_an_empty_tenant_does_not_crash(spark, good_code, make_cfg):
    """Re-running does not duplicate rows; a range without data writes an empty result."""
    cfg = make_cfg(tenant="gt")
    first = good_code.run(spark, cfg)
    second = good_code.run(spark, cfg)
    assert first == second
    output = spark.read.format("delta").load(f"{cfg.paths.root}/mentoring/gt/routine_deliveries")
    assert output.count() == first["written"]

    # bad_code.py raised IndexError when the filter left no rows; here it is just zero rows.
    empty = make_cfg(
        "execution.start_date=2024-01-01", "execution.end_date=2024-01-31", tenant="gt"
    )
    assert good_code.run(spark, empty)["written"] == 0
