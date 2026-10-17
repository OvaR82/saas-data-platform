"""storage.mode=catalog: the same pipeline addressing tables by Unity Catalog-style names.

Locally the catalog is Spark's own `spark_catalog` (managed tables in a temp warehouse); on
Databricks it would be `saas_<env>`. The names follow architecture 5.2, 5.6 and 5.9.
"""

import pytest

from saas_pipeline import gold, silver
from saas_pipeline.config import quality_logs_ref, quarantine_ref, table_ref
from saas_pipeline.pipeline import LAYER_ORDER, run_tenant

CATALOG = "spark_catalog"


@pytest.fixture
def catalog_cfg(spark, make_cfg):
    """Config for tenant sv in catalog mode; drops the schemas it created when the test ends."""
    cfg = make_cfg("storage.mode=catalog", f"storage.catalog={CATALOG}")
    yield cfg
    for schema in ("bronze_sv", "bronze_quarantine_sv", "silver_sv", "silver_quarantine_sv",
                   "gold_sv", "shared"):  # fmt: skip
        spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")


def test_refs_use_the_logical_names_of_the_architecture(catalog_cfg):
    """Tables, quarantine and quality_logs map to <catalog>.<layer>_<tenant>.<table> names."""
    assert table_ref(catalog_cfg, "silver", "fact_deliveries").name == (
        "spark_catalog.silver_sv.fact_deliveries"
    )
    assert quarantine_ref(catalog_cfg, "silver", "fact_deliveries").name == (
        "spark_catalog.silver_quarantine_sv.fact_deliveries"
    )
    assert quality_logs_ref(catalog_cfg).name == "spark_catalog.shared.quality_logs"


def test_full_run_by_table_name_is_idempotent_and_matches_path_mode(spark, catalog_cfg):
    """The whole pipeline runs by table name, twice, with the same counts as path mode."""

    def counts():
        """Row count of the main tables, read by name."""
        names = {
            "fact": f"{CATALOG}.silver_sv.{silver.FACT_TABLE}",
            "dim": f"{CATALOG}.silver_sv.{silver.DIM_TABLE}",
            "quarantine": f"{CATALOG}.silver_quarantine_sv.{silver.FACT_TABLE}",
            "gold": f"{CATALOG}.gold_sv.{gold.TABLE}",
            "gold_top": f"{CATALOG}.gold_sv.{gold.TOP_TABLE}",
        }
        return {key: spark.table(name).count() for key, name in names.items()}

    run_tenant(spark, catalog_cfg, list(LAYER_ORDER), "cat1")
    first = counts()
    run_tenant(spark, catalog_cfg, list(LAYER_ORDER), "cat2")

    assert counts() == first
    # Same numbers as the path-mode run of tenant sv (README / test_pipeline).
    assert first == {"fact": 722, "dim": 35, "quarantine": 20, "gold": 125, "gold_top": 30}

    logs = spark.table(f"{CATALOG}.shared.quality_logs").filter("_run_id = 'cat2'")
    assert logs.filter("check_severity = 'critical' AND NOT check_passed").count() == 0
    assert logs.filter("layer = 'gold'").count() == 1
