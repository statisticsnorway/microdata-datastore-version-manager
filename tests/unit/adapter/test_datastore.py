import shutil
import os
import pytest

from datastore_version_manager.adapter import datastore

DATASTORE_ROOT_DIR = 'tests/resources/SSB_FDB'


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


def test_draft_dataset_exists():
    assert datastore.draft_dataset_exists('TEST_DATASET')


def test_delete_draft_dataset_single_parquet():
    dataset_name = 'TEST_DATASET'
    draft_data_path = (
        f'{DATASTORE_ROOT_DIR}/data/{dataset_name}/'
        f'{dataset_name}__DRAFT.parquet'
    )
    draft_metadata_path = (
        f'{DATASTORE_ROOT_DIR}/metadata/{dataset_name}/'
        f'{dataset_name}__DRAFT.json'
    )
    datastore.delete_draft_dataset(dataset_name)
    assert not os.path.exists(draft_data_path)
    assert not os.path.exists(draft_metadata_path)


def test_delete_draft_dataset_partitioned_parquet():
    dataset_name = 'TEST_DATASET_PARTITIONED'
    draft_data_path = (
        f'{DATASTORE_ROOT_DIR}/data/{dataset_name}/'
        f'{dataset_name}__DRAFT'
    )
    draft_metadata_path = (
        f'{DATASTORE_ROOT_DIR}/metadata/{dataset_name}/'
        f'{dataset_name}__DRAFT.json'
    )
    datastore.delete_draft_dataset(dataset_name)
    assert not os.path.exists(draft_data_path)
    assert not os.path.exists(draft_metadata_path)


def test_is_dataset_in_datastore_versions():
    assert datastore.is_dataset_in_datastore_versions(
        'SKATT_BRUTTOINNTEKT', 'RELEASED'
    )
    assert not datastore.is_dataset_in_datastore_versions(
        'SKATT_BRUTTOINNTEKT', 'DELETED'
    )
