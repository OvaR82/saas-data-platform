from pathlib import Path

import pytest

from saas_pipeline.config import load_config
from saas_pipeline.session import get_spark

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO_ROOT / "config"


@pytest.fixture(scope="session")
def spark():
    return get_spark("saas-pipeline-tests")


@pytest.fixture
def make_cfg(tmp_path):
    """Config for tenant `sv` writing into a temporary directory."""

    def _make(*overrides: str, tenant: str = "sv"):
        base = [
            f"paths.root={tmp_path.as_posix()}",
            f"paths.raw={(REPO_ROOT / 'data/raw').as_posix()}",
        ]
        return load_config("dev", tenant, [*base, *overrides], config_dir=CONFIG_DIR)

    return _make
