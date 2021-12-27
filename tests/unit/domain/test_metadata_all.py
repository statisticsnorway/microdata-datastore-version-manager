import pytest
import shutil
from datastore_version_manager.domain import metadata_all
from datastore_version_manager.adapter import datastore


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


def test_generate_metadata_all_draft():
    metadata_all.generate_metadata_all_draft()
    metadata_all_draft = datastore.get_metadata_all('draft')
    data_structures = [
        data_structure['name']
        for data_structure in metadata_all_draft['dataStructures']
    ]
    assert data_structures == [
        'PERSON_SIVILSTAND', 'TEST_DATASET', 'ANOTHER_TEST_DATASET'
    ]
