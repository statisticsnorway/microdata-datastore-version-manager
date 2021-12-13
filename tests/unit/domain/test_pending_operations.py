import pytest
from datastore_version_manager.adapter import datastore
from datastore_version_manager.domain import pending_operations


@pytest.fixture(autouse=True)
def setup_environment(monkeypatch):
    monkeypatch.setenv('DATASTORE_ROOT_DIR', 'tests/resources/SSB_FDB')
    monkeypatch.setenv('DATASET_BUILDER_OUTPUT_DIR', 'tests/resources/built_datasets')


def test_remove_dataset_from_pending_operations():
    dataset_name = 'ANOTHER_TEST_DATASET'
    pending_operations.remove(dataset_name)
    pending_operations_list = datastore.get_pending_operations()["pendingOperations"]
    assert len(pending_operations_list) == 1
    if datastore.draft_dataset_exists(dataset_name):
        assert False


def test_remove_non_existing_dataset_from_pending_operations():
    with pytest.raises(datastore.DatasetNotFound) as e:
        pending_operations.remove("DOES_NOT_EXIST")
    assert "Dataset DOES_NOT_EXIST not found in pending_operations.json" in str(e.value)