import pytest
import shutil
import json
from datastore_version_manager import commands
from datastore_version_manager.adapter import datastore
from datastore_version_manager.commands import ForbiddenOperation
from datastore_version_manager.adapter.built_datasets import (
    NoBuiltDataset
)
from datastore_version_manager.domain.pending_operations import (
    ReleaseStatusTransitionNotAllowed
)

PENDING_OPERATIONS_FILE_PATH = (
    'tests/resources/SSB_FDB/datastore/pending_operations.json'
)


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
    monkeypatch.setenv('BUILT_DATASETS_DIR', 'tests/resources/built_datasets')


def test_update_release_status():
    commands.set_status(
        'TEST_DATASET', 'PENDING_RELEASE', 'ADD', 'Nytt datasett om test'
    )

    with open(PENDING_OPERATIONS_FILE_PATH) as f:
        pending_operations = json.load(f)

    assert pending_operations["version"] == "0.0.0.2"
    assert pending_operations["updateType"] == "MAJOR"
    assert {
               "name": "TEST_DATASET",
               "operation": "ADD",
               "description": "Nytt datasett om test",
               "releaseStatus": "PENDING_RELEASE"
           } in pending_operations["dataStructureUpdates"]


def test_update_release_status_not_allowed():
    with pytest.raises(ReleaseStatusTransitionNotAllowed):
        commands.set_status('TEST_DATASET', 'DRAFT', 'ADD')


def test_update_release_status_pending_delete():
    commands.set_status(
        'PERSON_SIVILSTAND', 'PENDING_DELETE', 'REMOVE', 'Fjernet'
    )

    with open(PENDING_OPERATIONS_FILE_PATH) as f:
        pending_operations = json.load(f)
    draft_metadata_all = datastore.get_metadata_all('draft')

    assert pending_operations["version"] == "0.0.0.2"
    assert pending_operations["updateType"] == "MAJOR"
    assert {
               "name": "PERSON_SIVILSTAND",
               "operation": "REMOVE",
               "description": "Fjernet",
               "releaseStatus": "PENDING_DELETE"
           } in pending_operations["dataStructureUpdates"]
    assert not any([
        data_structure["name"] == 'PERSON_SIVILSTAND'
        for data_structure in draft_metadata_all["dataStructures"]
    ])


def test_add_new_dataset():
    commands.add_new_dataset('NEW_VARIABLE', 'Første variabel', 'ADD')

    with open(PENDING_OPERATIONS_FILE_PATH) as f:
        pending_operations = json.load(f)
    draft_metadata_all = datastore.get_metadata_all('draft')

    assert pending_operations["version"] == "0.0.0.2"
    assert pending_operations["updateType"] == "MAJOR"
    assert {
               "name": "NEW_VARIABLE",
               "operation": "ADD",
               "description": "Første variabel",
               "releaseStatus": "DRAFT"
           } in pending_operations["dataStructureUpdates"]
    assert any([
        data_structure["name"] == 'NEW_VARIABLE'
        for data_structure in draft_metadata_all["dataStructures"]
    ])


def test_add_new_dataset_already_versioned():
    with pytest.raises(ForbiddenOperation) as e:
        commands.add_new_dataset(
            'SKATT_BRUTTOINNTEKT', 'Finnes allerede', 'ADD'
        )
    assert (
        "A versioned variable of the same name already exists in datastore"
    ) in str(e.value)


def test_add_new_dataset_not_built():
    with pytest.raises(NoBuiltDataset) as e:
        commands.add_new_dataset(
            'NOT_BUILT', 'finnes ikke', 'ADD'
        )
    assert "No built data file for NOT_BUILT" in str(e.value)
