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
    ReleaseStatusTransitionNotAllowed, PendingOperationException
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


def test_update_pending_operation():
    draft_dataset.update_pending_operation(
        'TEST_DATASET', 'PENDING_RELEASE', 'Nytt datasett om test'
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


def test_update_pending_operation_transition_not_allowed():
    with pytest.raises(ReleaseStatusTransitionNotAllowed):
        draft_dataset.update_pending_operation(
            'TEST_DATASET', 'DRAFT'
        )


def test_update_pending_operation_with_nonexisting_dataset():
    with pytest.raises(PendingOperationException):
        draft_dataset.update_pending_operation(
            'DATASET_NOT_ADDED_BEFORE', 'DRAFT'
        )


def test_add_new_draft_dataset():
    draft_dataset.add_new_draft_dataset(
        'ADD_OR_CHANGE_DATA', 'NEW_VARIABLE', 'Første variabel'
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
        'ADD_OR_CHANGE_DATA', 'PERSON_SIVILSTAND', 'Nye årganger'
    )

    pending_operations = read_pending_operations_from_file()
    draft_metadata_all = datastore.get_metadata_all('DRAFT')

    assert pending_operations["version"] == f"0.0.0.{pending_operations['releaseTime']}"
    assert pending_operations["updateType"] == "MAJOR"
    assert {
               "name": "PERSON_SIVILSTAND",
               "operation": "CHANGE_DATA",
               "description": "Nye årganger",
               "releaseStatus": "DRAFT"
           } in pending_operations["dataStructureUpdates"]
    assert any([
        data_structure["name"] == 'PERSON_SIVILSTAND'
        for data_structure in draft_metadata_all["dataStructures"]
    ])


def test_add_new_draft_dataset_already_versioned():
    with pytest.raises(ForbiddenOperation) as e:
        draft_dataset.add_new_draft_dataset(
            'ADD_OR_CHANGE_DATA', 'SKATT_BRUTTOINNTEKT', 'Finnes allerede'
        )
    assert (
        "It exists in the draft version of datastore"
    ) in str(e.value)


def test_add_new_draft_dataset_deleted_in_datastore():
    with pytest.raises(ForbiddenOperation) as e:
        draft_dataset.add_new_draft_dataset(
            'ADD_OR_CHANGE_DATA', 'DELETED_DATASET', 'Nye data'
        )
    assert (
        "This variable has been removed from datastore"
    ) in str(e.value)


def test_add_new_draft_dataset_not_built():
    with pytest.raises(NoBuiltDataset) as e:
        draft_dataset.add_new_draft_dataset(
            'ADD_OR_CHANGE_DATA', 'NOT_BUILT', 'finnes ikke'
        )
    assert "No built data file for NOT_BUILT" in str(e.value)


def read_pending_operations_from_file():
    with open(PENDING_OPERATIONS_FILE_PATH) as f:
        pending_operations = json.load(f)
    return pending_operations
