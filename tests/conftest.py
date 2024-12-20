import pytest

from tests._mocks import TMP_PATH, MockDbtMetabase, MockMetabase


@pytest.fixture(name="core")
def fixture_core() -> MockDbtMetabase:
    c = MockDbtMetabase()
    c._ModelsMixin__SYNC_PERIOD = 1  # type: ignore
    return c


@pytest.fixture(name="metabase")
def fixture_metabase() -> MockMetabase:
    return MockMetabase(url="http://localhost")


def setup_module():
    TMP_PATH.mkdir(exist_ok=True)
