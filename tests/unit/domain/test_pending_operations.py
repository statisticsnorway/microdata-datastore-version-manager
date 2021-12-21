import pytest
import os
import shutil
from datastore_version_manager.adapter import datastore
from datastore_version_manager.domain import pending_operations


DATASTORE_ROOT_DIR = 'tests/resources/SSB_FDB'
ARCHIVE_DIR = f'{DATASTORE_ROOT_DIR}/archive/pending_operations'


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
    monkeypatch.setenv('DATASTORE_ROOT_DIR', DATASTORE_ROOT_DIR)
    monkeypatch.setenv(
        'DATASET_BUILDER_OUTPUT_DIR', 'tests/resources/built_datasets'
    )


def test_remove_dataset_from_pending_operations():
    dataset_name = 'ANOTHER_TEST_DATASET'
    pending_operations.remove(dataset_name)

    actual_pending_operations = datastore.get_pending_operations()

    assert actual_pending_operations["updateType"] == ''
    assert os.path.exists(f'{ARCHIVE_DIR}/pending_operation__0_0_0_1.json')
    assert len(actual_pending_operations["dataStructureUpdates"]) == 1
    if datastore.draft_dataset_exists(dataset_name):
        assert False


def test_remove_non_existing_dataset_from_pending_operations():
    with pytest.raises(datastore.DatasetNotFound) as e:
        pending_operations.remove("DOES_NOT_EXIST")
    assert (
        "Dataset DOES_NOT_EXIST not found in pending_operations.json"
    ) in str(e.value)


def test_get_release_status():
    dataset_name = 'ANOTHER_TEST_DATASET'
    release_status = pending_operations.get_release_status(dataset_name)
    assert release_status == 'DRAFT'


def test_get_release_status_not_exist():
    dataset_name = 'DOES_NOT_EXIST'
    release_status = pending_operations.get_release_status(dataset_name)
    assert release_status is None
