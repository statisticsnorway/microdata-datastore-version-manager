import shutil
import os
import pytest

from datastore_version_manager.adapter import datastore
from datastore_version_manager.adapter.datastore import DatasetNotFound


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


def test_draft_dataset_exists():
    assert datastore.draft_dataset_exists('TEST_DATASET') == True


def test_delete_draft_dataset_single_parquet():
    dataset_name = 'TEST_DATASET'
    datastore.delete_draft_dataset(dataset_name)
    assert os.path.isdir(datastore.get_metadata_dir_path(dataset_name)) == False
    assert os.path.isdir(datastore.get_data_dir_path(dataset_name)) == False


def test_delete_draft_dataset_partitioned_parquet():
    dataset_name = 'TEST_DATASET_PARTITIONED'
    datastore.delete_draft_dataset(dataset_name)
    assert os.path.isdir(datastore.get_metadata_dir_path(dataset_name)) == False
    assert os.path.isdir(datastore.get_data_dir_path(dataset_name)) == False


def test_remove_dataset_from_pending_operations():
    dataset_name = 'TEST_DATASET'
    datastore.remove_dataset_from_pending_operations(dataset_name)
    pending_operations_list = datastore.get_pending_operations()["dataStructureUpdates"]
    assert len(pending_operations_list) == 1
    if datastore.draft_dataset_exists(dataset_name):
        assert False


def test_is_dataset_in_data_store():
    assert datastore.is_dataset_in_data_store('SKATT_BRUTTOINNTEKT', 'RELEASED') == True
    assert datastore.is_dataset_in_data_store('SKATT_BRUTTOINNTEKT', 'DELETED') == False


def test_remove_non_existing_dataset_from_pending_operations():
    try:
        datastore.remove_dataset_from_pending_operations("DOES_NOT_EXIST")
    except DatasetNotFound:
        assert True
