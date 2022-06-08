import pytest
import shutil
from datastore_version_manager.domain import metadata_all
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


def test_generate_metadata_all_draft():
    metadata_all.generate_metadata_all_draft()
    metadata_all_draft = datastore.get_metadata_all('DRAFT')
    data_structures = [
        data_structure['name']
        for data_structure in metadata_all_draft['dataStructures']
    ]
    assert data_structures == [
        'PERSON_SIVILSTAND', 'TEST_DATASET_3', 'TEST_DATASET', 'ANOTHER_TEST_DATASET'
    ]


def test_create_new_version():
    data_structures = [
        {
            "description": "Nye data",
            "name": "PERSON_SIVILSTAND",
            "operation": "CHANGE_DATA",
            "releaseStatus": "RELEASED"
        }
    ]
    metadata_all.create_new_version(data_structures, "2.0.0.0", "3.0.0.0")

    metadata_all_3 = datastore.get_metadata_all('3.0.0.0')
    data_structures = [
        data_structure for data_structure in metadata_all_3['dataStructures']
        if data_structure["name"] == "PERSON_SIVILSTAND"
    ]
    assert data_structures[0] == {
            "description": "Nye data",
            "name": "PERSON_SIVILSTAND",
            "operation": "CHANGE_DATA",
            "releaseStatus": "RELEASED"
        }
