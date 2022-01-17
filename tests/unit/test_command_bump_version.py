import os
import shutil

import pytest

from datastore_version_manager import commands
from datastore_version_manager.adapter import datastore


def setup_function():
    shutil.copytree(
        'tests/resources/bump_version',
        'tests/resources/bump_version_backup'
    )


def teardown_function():
    shutil.rmtree('tests/resources/bump_version')
    shutil.move(
        'tests/resources/bump_version_backup',
        'tests/resources/bump_version'
    )


@pytest.fixture(autouse=True)
def setup_environment(monkeypatch):
    monkeypatch.setenv('DATASTORE_ROOT_DIR', 'tests/resources/bump_version')


def test_bump_version():
    pending_operations = datastore.get_pending_operations()
    previous_release_time = pending_operations["releaseTime"]

    bump_desc = "First version"
    commands.bump_version(bump_desc)

    # pending_operations.json file - updated
    pending_operations = datastore.get_pending_operations()
    assert pending_operations["version"] == "0.0.0.2"
    assert pending_operations["releaseTime"] > previous_release_time
    assert pending_operations["updateType"] == "MINOR"  # still one dataset in DRAFT, else it would be empty
    assert pending_operations["dataStructureUpdates"][0] == {
               "name": "DATASET_B",
               "operation": "ADD",
               "description": "Dataset B",
               "releaseStatus": "DRAFT"
           }

    # data_store.json file - updated
    datastore_info = datastore.get_datastore_info()
    assert len(datastore_info["versions"]) == 1
    assert datastore_info["versions"][0] == {
        "version": "1.0.0.0",
        "description": bump_desc,
        "releaseTime": pending_operations["releaseTime"],
        "languageCode": "no",
        "dataStructureUpdates": [
            {
                "dataSetName": "DATASET_A",
                "operation": "ADD",
                "description": "Dataset A",
                "releaseStatus": "RELEASED"
            }
        ]
    }

    # metadata_all__1_0_0.json file - created
    metadata_all = datastore.get_metadata_all("1.0.0")
    assert len(metadata_all["dataStructures"]) == 1
    assert metadata_all["dataStructures"][0]["name"] == "DATASET_A"
    assert metadata_all["dataStructures"][0]["identifierVariables"]  # exists

    # metadata_all_draft.json file - updated
    metadata_all_draft = datastore.get_metadata_all('draft')
    assert len(metadata_all_draft["dataStructures"]) == 1
    assert metadata_all_draft["dataStructures"][0]["name"] == "DATASET_A"
    assert metadata_all_draft["dataStructures"][0]["identifierVariables"]  # exists

    # Redundant metadata file DATASET_A__0_0_0.json - change in name
    assert not os.path.exists(datastore.create_metadata_file_path("DATASET_A", "0.0.0"))
    assert os.path.exists(datastore.create_metadata_file_path("DATASET_A", "1.0.0"))

    # Data file DATASET_A__0_0_0.parquet (or directory) - change in name
    assert not os.path.exists(datastore.create_data_file_path("DATASET_A", "0.0.0", False))
    assert os.path.exists(datastore.create_data_file_path("DATASET_A", "1.0.0", False))

    # data_versions__x_x_x.json - created
    data_versions = datastore.get_data_versions("1.0.0")
    assert len(data_versions) == 1
    assert data_versions["DATASET_A"] == "DATASET_A__1_0_0.parquet"
