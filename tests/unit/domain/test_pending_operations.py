import pytest
import shutil
from datastore_version_manager.adapter import datastore
from datastore_version_manager.domain import pending_operations


def setup_function():
    shutil.copytree(
        'tests/resources',
        'tests/resources_backup'
    )


def teardown_function():
    shutil.rmtree('tests/resources')
    shutil.move(
        'tests/resources_backup',
        'tests/resources'
    )


@pytest.fixture(autouse=True)
def setup_environment(monkeypatch):
    monkeypatch.setenv('DATASTORE_ROOT_DIR', 'tests/resources/SSB_FDB')
    monkeypatch.setenv('DATASET_BUILDER_OUTPUT_DIR', 'tests/resources/built_datasets')


def test_remove_dataset_from_pending_operations():
    dataset_name = 'ANOTHER_TEST_DATASET'
    pending_operations.remove(dataset_name)
    data_structure_updates = datastore.get_pending_operations()["dataStructureUpdates"]
    assert len(data_structure_updates) == 1
    if datastore.draft_dataset_exists(dataset_name):
        assert False


def test_remove_non_existing_dataset_from_pending_operations():
    with pytest.raises(datastore.DatasetNotFound) as e:
        pending_operations.remove("DOES_NOT_EXIST")
    assert "Dataset DOES_NOT_EXIST not found in pending_operations.json" in str(e.value)
