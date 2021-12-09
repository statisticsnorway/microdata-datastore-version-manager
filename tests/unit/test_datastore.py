import shutil
import os
import pytest

from datastore_version_manager.adapter import datastore


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
    dataset_name = 'TEST_DATASET'
    datastore.draft_dataset_exists(dataset_name)
    assert datastore.draft_dataset_exists(dataset_name) == True


def test_draft_dataset_delete():
    dataset_name = 'TEST_DATASET'
    datastore.draft_dataset_delete(dataset_name)
    assert os.path.isdir(datastore.get_metadata_dir_path(dataset_name)) == False
    assert os.path.isdir(datastore.get_data_dir_path(dataset_name)) == False

