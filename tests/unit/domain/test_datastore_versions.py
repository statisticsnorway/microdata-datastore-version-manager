import pytest
from datastore_version_manager.domain import datastore_versions


@pytest.fixture(autouse=True)
def setup_environment(monkeypatch):
    monkeypatch.setenv('DATASTORE_ROOT_DIR', 'tests/resources/SSB_FDB')


def test_get_released_datasets():
    expected = [{'datasetName': 'PERSON_SIVILSTAND', 'version': '2.0.0.0', 'operation': 'ADD'},
                {'datasetName': 'SKATT_BRUTTOINNTEKT', 'version': '2.0.0.0', 'operation': 'CHANGE_DATA'}]

    actual = datastore_versions.get_released_datasets()
    assert actual == expected
