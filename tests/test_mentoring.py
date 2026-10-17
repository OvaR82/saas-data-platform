"""mentoring/good_code.py: same business result as bad_code.py, without its silent errors."""

import importlib.util
from decimal import Decimal
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DELIVERIES_CSV = (REPO_ROOT / "data/raw/global_mobility_data_entrega_productos.csv").as_posix()


@pytest.fixture(scope="module")
def good_code():
    """Import mentoring/good_code.py by path (mentoring/ is not a package)."""
    spec = importlib.util.spec_from_file_location(
        "good_code", REPO_ROOT / "mentoring/good_code.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def row(
    country="GT",
    process_date="20250314",
    delivery_type="ZPRE",
    price="1.5",
    quantity="2",
    unit="CS",
    route=1,
):
    """Return one source row; each test overrides only the fields it cares about."""
    return (
        country,
        process_date,
        1,
        route,
        delivery_type,
        "AA004003",
        Decimal(price) if price else None,
        Decimal(quantity) if quantity else None,
        unit,
    )


def test_keeps_the_original_business_rules(spark, good_code):
    """Routine only, CS x 20, total = units x price, and 'GT'/'gt' are the same tenant."""
    rows = [
        row(unit="CS", route=1),  # 2 CS -> 40 ST, total 60
        row(country="gt", unit="ST", quantity="5", route=2),  # lowercase source still matches
        row(delivery_type="Z04", route=3),  # bonus: not part of this report
        row(country="SV", route=4),  # another tenant
    ]
    valid, rejected = good_code.routine_deliveries(
        spark.createDataFrame(rows, good_code.SCHEMA), "gt"
    )

    assert {r.cantidad_st: r.total for r in valid.collect()} == {
        Decimal(40): Decimal(60),
        Decimal(5): Decimal("7.5"),
    }
    assert rejected.count() == 0


def test_rejects_what_the_original_published_silently(spark, good_code):
    """Rows that bad_code.py would have published with wrong values are rejected with a reason."""
    rows = [
        row(quantity="-3", route=1),
        row(quantity=None, route=2),
        row(unit="KG", route=3),  # bad_code.py counted any non-CS unit as ST
        row(price=None, route=4),
        row(process_date=None, route=5),
        row(process_date="2025-03-15", route=6),
        row(route=7),
        row(route=7),  # exact duplicate: kept once
    ]
    valid, rejected = good_code.routine_deliveries(
        spark.createDataFrame(rows, good_code.SCHEMA), "gt"
    )

    assert [r.cantidad_st for r in valid.collect()] == [Decimal(40)]
    assert {r.ruta: r.reject_reason for r in rejected.collect()} == {
        1: "invalid_cantidad",
        2: "invalid_cantidad",
        3: "invalid_unidad",
        4: "null_precio",
        5: "invalid_fecha",
        6: "invalid_fecha",
    }


def test_process_is_idempotent_and_an_empty_tenant_does_not_crash(spark, good_code, tmp_path):
    """Re-running does not duplicate rows; a tenant without rows writes an empty table."""
    output = tmp_path.as_posix()
    first = good_code.process(spark, DELIVERIES_CSV, output, "gt")
    second = good_code.process(spark, DELIVERIES_CSV, output, "gt")

    assert first == second
    assert spark.read.format("delta").load(f"{output}/gt").count() == first["written"]
    # bad_code.py raised IndexError when the filter left no rows; here it is just zero rows.
    assert good_code.process(spark, DELIVERIES_CSV, output, "cr") == {"written": 0, "rejected": 0}
