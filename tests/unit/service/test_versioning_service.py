import pytest
import shutil
import json

from datastore_version_manager.adapter import datastore
from datastore_version_manager.domain import draft_dataset
from datastore_version_manager.exceptions.exceptions import ForbiddenOperation
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


def test_update_release_status():
    draft_dataset.update_pending_operation(
        'TEST_DATASET', 'PENDING_RELEASE', 'ADD', 'Nytt datasett om test'
    )

    pending_operations = read_pending_operations_from_file()

    assert pending_operations["version"] == f"0.0.0.{pending_operations['releaseTime']}"
    assert pending_operations["updateType"] == "MAJOR"
    assert {
               "name": "TEST_DATASET",
               "operation": "ADD",
               "description": "Nytt datasett om test",
               "releaseStatus": "PENDING_RELEASE"
           } in pending_operations["dataStructureUpdates"]


def test_update_release_status_not_allowed():
    with pytest.raises(ReleaseStatusTransitionNotAllowed):
        draft_dataset.update_pending_operation('TEST_DATASET', 'DRAFT', 'ADD')


def test_update_release_status_pending_delete():
    draft_dataset.update_pending_operation(
        'PERSON_SIVILSTAND', 'PENDING_DELETE', 'REMOVE', 'Fjernet'
    )

    pending_operations = read_pending_operations_from_file()
    draft_metadata_all = datastore.get_metadata_all('DRAFT')

    assert pending_operations["version"] == f"0.0.0.{pending_operations['releaseTime']}"
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


def test_add_new_draft_dataset():
    draft_dataset.add_new_draft_dataset(
        'ADD', 'NEW_VARIABLE', 'Første variabel', False
    )

    pending_operations = read_pending_operations_from_file()
    draft_metadata_all = datastore.get_metadata_all('DRAFT')

    assert pending_operations["version"] == f"0.0.0.{pending_operations['releaseTime']}"
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


def test_add_new_draft_dataset_change_data():
    draft_dataset.add_new_draft_dataset(
        'CHANGE_DATA', 'TEST_DATASET', 'Nye årganger', True
    )

    pending_operations = read_pending_operations_from_file()
    draft_metadata_all = datastore.get_metadata_all('DRAFT')

    assert pending_operations["version"] == f"0.0.0.{pending_operations['releaseTime']}"
    assert pending_operations["updateType"] == "MAJOR"
    assert {
               "name": "TEST_DATASET",
               "operation": "CHANGE_DATA",
               "description": "Nye årganger",
               "releaseStatus": "DRAFT"
           } in pending_operations["dataStructureUpdates"]
    assert any([
        data_structure["name"] == 'TEST_DATASET'
        for data_structure in draft_metadata_all["dataStructures"]
    ])


def test_add_new_draft_dataset_already_versioned():
    with pytest.raises(ForbiddenOperation) as e:
        draft_dataset.add_new_draft_dataset(
            'ADD', 'SKATT_BRUTTOINNTEKT', 'Finnes allerede', False
        )
    assert (
        "A versioned variable of the same name already exists in datastore"
    ) in str(e.value)


def test_add_new_draft_dataset_not_built():
    with pytest.raises(NoBuiltDataset) as e:
        draft_dataset.add_new_draft_dataset(
            'ADD', 'NOT_BUILT', 'finnes ikke', False
        )
    assert "No built data file for NOT_BUILT" in str(e.value)


def read_pending_operations_from_file():
    with open(PENDING_OPERATIONS_FILE_PATH) as f:
        pending_operations = json.load(f)
    return pending_operations
