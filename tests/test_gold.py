"""Gold top_materials_by_month: ranking, SCD2 attributes and whole-month reprocessing."""

from datetime import date
from decimal import Decimal

from saas_pipeline import gold
from saas_pipeline.config import table_path
from saas_pipeline.pipeline import LAYER_ORDER, run_tenant

FACT_COLUMNS = (
    "tenant_id string, fecha_proceso string, fecha_proceso_date date, material string, "
    "descripcion string, categoria string, precio decimal(38,18), "
    "cantidad_normalizada_st decimal(38,18)"
)


def fact_row(fecha, material, revenue, categoria="CAT"):
    """Return one Silver fact row with 1 unit at `revenue`, so revenue is easy to reason about."""
    day = date(int(fecha[:4]), int(fecha[4:6]), int(fecha[6:]))
    return (
        "sv",
        fecha,
        day,
        material,
        f"desc {material}",
        categoria,
        Decimal(revenue),
        Decimal(1),
    )


def test_top_materials_ranks_per_month_and_keeps_top_n(spark):
    """Materials are ranked by revenue inside each month; only the top N are kept."""
    rows = [
        fact_row("20250301", "A", "50"),
        fact_row("20250315", "A", "10"),  # A: 60 in March
        fact_row("20250310", "B", "30"),
        fact_row("20250310", "C", "10"),  # C is 3rd in March -> dropped with top_n=2
        fact_row("20250401", "C", "70"),  # April is ranked on its own
    ]
    df = spark.createDataFrame(rows, FACT_COLUMNS)
    out = gold.build_top_materials(df, top_n=2).collect()

    ranking = {(r.month_start.month, r.revenue_rank): r.material for r in out}
    assert ranking == {(3, 1): "A", (3, 2): "B", (4, 1): "C"}
    march_a = next(r for r in out if r.material == "A" and r.month_start.month == 3)
    assert march_a.total_revenue == Decimal(60)
    assert march_a.revenue_share == Decimal("0.6")  # 60 of a 100 March total, before the cut


def test_top_materials_reports_the_latest_version_in_the_month(spark):
    """If the material changes version mid-month, the latest category of that month is used."""
    rows = [
        fact_row("20250305", "A", "10", categoria="OLD"),
        fact_row("20250320", "A", "10", categoria="NEW"),
    ]
    out = gold.build_top_materials(spark.createDataFrame(rows, FACT_COLUMNS), top_n=5).collect()
    assert [r.categoria for r in out] == ["NEW"]


def test_month_bounds_expand_to_whole_months():
    """A mid-month range is widened to complete months (including leap years)."""
    assert gold.month_bounds("20250310", "20250320") == (date(2025, 3, 1), date(2025, 3, 31))
    assert gold.month_bounds("20240215", "20240405") == (date(2024, 2, 1), date(2024, 4, 30))
    assert gold.month_bounds("20240201", "20240229") == (date(2024, 2, 1), date(2024, 2, 29))


def test_partial_range_recomputes_the_whole_month(spark, make_cfg):
    """Reprocessing 10-20 March gives the same March ranking as processing the full month."""
    full = make_cfg()
    run_tenant(spark, full, list(LAYER_ORDER), "full")
    path = table_path(full, "gold", gold.TOP_TABLE)

    def march(batch):
        """March ranking as (rank, material, revenue) rows, plus the batch that wrote it."""
        rows = spark.read.format("delta").load(path).filter("month_start = DATE'2025-03-01'")
        ranking = sorted((r.revenue_rank, r.material, r.total_revenue) for r in rows.collect())
        return ranking, {r._batch_id for r in rows.collect()} == {f"{batch}_sv"}

    before, _ = march("full")
    partial = make_cfg("execution.start_date=2025-03-10", "execution.end_date=2025-03-20")
    run_tenant(spark, partial, ["gold"], "partial")
    after, rewritten = march("partial")

    assert rewritten  # March was recomputed by the partial run...
    assert after == before  # ...from the whole month, so the ranking did not change
    other_months = spark.read.format("delta").load(path).filter("month_start != DATE'2025-03-01'")
    assert {r._batch_id for r in other_months.collect()} == {"full_sv"}
