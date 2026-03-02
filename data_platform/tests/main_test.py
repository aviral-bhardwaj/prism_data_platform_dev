import os
import pytest

from data_plat_demo.main import get_taxis, get_spark


@pytest.mark.skipif(
    os.environ.get("CI") == "true" or os.environ.get("GITHUB_ACTIONS") == "true",
    reason="Requires live Databricks cluster connection",
)
def test_main():
    taxis = get_taxis(get_spark())
    assert taxis.count() > 5
