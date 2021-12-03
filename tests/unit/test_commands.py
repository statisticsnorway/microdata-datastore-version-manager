import pytest
import shutil
import json
from datastore_version_manager import commands
from datastore_version_manager.adapter.pending_operations import ReleaseStatusTransitionNotAllowed


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


def test_update_release_status():
    commands.set_status_for_pending_operation('TEST_DATASET', 'PENDING_RELEASE')

    with open('tests/resources/SSB_FDB/datastore/pending_operations.json') as f:
        pending_operations = json.load(f)
    assert pending_operations["version"] == "0.0.0.1"
    assert {
               "datasetName": "TEST_DATASET",
               "operation": "ADD",
               "description": "Nytt datasett om test",
               "releaseStatus": "PENDING_RELEASE"
           } in pending_operations["pendingOperations"]


def test_update_release_status_not_allowed():
    with pytest.raises(ReleaseStatusTransitionNotAllowed):
        commands.set_status_for_pending_operation('TEST_DATASET', 'DRAFT')


def test_new_draft_to_datastore():
    commands.new_draft_to_datastore('NEW_VARIABLE', 'Første variabel', 'ADD')
    with open('tests/resources/SSB_FDB/datastore/pending_operations.json') as f:
        pending_operations = json.load(f)
    assert pending_operations["version"] == "0.0.0.1"
    assert {
               "datasetName": "NEW_VARIABLE",
               "operation": "ADD",
               "description": "Første variabel",
               "releaseStatus": "DRAFT"
           } in pending_operations["pendingOperations"]
