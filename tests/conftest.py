"""Shared pytest fixtures: one local Spark session and isolated configs per test."""

from pathlib import Path

import pytest

from saas_pipeline.config import load_config
from saas_pipeline.session import get_spark

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO_ROOT / "config"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """A single SparkSession for the whole test run: starting the JVM is the slow part."""
    # Catalog-mode tests create managed tables: keep them in a temp warehouse, not the repo.
    warehouse = tmp_path_factory.mktemp("spark-warehouse").as_posix()
    session = get_spark("saas-pipeline-tests", warehouse_dir=warehouse)
    # Tests run in ANSI mode, the stricter default of Databricks serverless: invalid casts and
    # dates raise errors there, so the code must stay safe in both modes.
    session.conf.set("spark.sql.ansi.enabled", "true")
    return session


@pytest.fixture
def make_cfg(tmp_path):
    """Config for tenant `sv` writing into a temporary directory."""

    def _make(*overrides: str, tenant: str = "sv"):
        """Build a config for `tenant` (default sv) with extra dotlist `overrides` applied."""
        # Outputs go to pytest's tmp_path; inputs are the real CSVs of the repository.
        base = [
            f"paths.root={tmp_path.as_posix()}",
            f"paths.raw={(REPO_ROOT / 'data/raw').as_posix()}",
        ]
        return load_config("dev", tenant, [*base, *overrides], config_dir=CONFIG_DIR)

    return _make
