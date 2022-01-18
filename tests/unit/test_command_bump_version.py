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

    assert len(datastore.get_archive('pending_operations')) == 3

    bump_desc = "First version"
    commands.bump_version(bump_desc)

    assert len(datastore.get_archive('pending_operations')) == 0

    # pending_operations.json file - updated
    pending_operations = datastore.get_pending_operations()
    assert pending_operations["version"] == "0.0.0.4"
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
        "version": "0.1.0.0",
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
    metadata_all = datastore.get_metadata_all("0.1.0")
    assert len(metadata_all["dataStructures"]) == 1
    assert metadata_all["dataStructures"][0]["name"] == "DATASET_A"
    assert metadata_all["dataStructures"][0]["identifierVariables"]  # exists

    # metadata_all_draft.json file - updated
    metadata_all_draft = datastore.get_metadata_all('draft')
    assert len(metadata_all_draft["dataStructures"]) == 2
    dataset_names = [
        data_structure["name"] for data_structure in metadata_all_draft["dataStructures"]
    ]
    assert "DATASET_A" and "DATASET_B" in dataset_names

    # Redundant metadata file DATASET_A__0_0_0.json - change in name
    assert not os.path.exists(datastore.create_metadata_file_path("DATASET_A", "0.0.0"))
    assert os.path.exists(datastore.create_metadata_file_path("DATASET_A", "0.1.0"))

    # Data file DATASET_A__0_0_0.parquet (or directory) - change in name
    assert not os.path.exists(datastore.create_data_file_path("DATASET_A", "0.0.0", False))
    assert os.path.exists(datastore.create_data_file_path("DATASET_A", "0.1.0", False))

    # data_versions__0_1_0.json - created
    data_versions = datastore.get_data_versions("0.1.0")
    assert len(data_versions) == 1
    assert data_versions["DATASET_A"] == "DATASET_A__0_1.parquet"


def test_bump_version_twice():
    bump_desc = "First version"
    commands.bump_version(bump_desc)

    commands.set_status("DATASET_B", "PENDING_RELEASE", "ADD")

    assert len(datastore.get_archive('pending_operations')) == 1

    pending_operations = datastore.get_pending_operations()
    assert pending_operations["version"] == "0.0.0.5"
    assert pending_operations["dataStructureUpdates"][0] == {
        "name": "DATASET_B",
        "operation": "ADD",
        "description": "Dataset B",
        "releaseStatus": "PENDING_RELEASE"
    }

    bump_desc = "Second version"
    commands.bump_version(bump_desc)

    assert len(datastore.get_archive('pending_operations')) == 0

    # pending_operations.json file - updated
    pending_operations = datastore.get_pending_operations()
    assert pending_operations["version"] == "0.0.0.6"
    assert pending_operations["updateType"] == ""  # no datasets
    assert len(pending_operations["dataStructureUpdates"][0]) == 0

    # data_store.json file - updated
    datastore_info = datastore.get_datastore_info()
    assert len(datastore_info["versions"]) == 2
    assert datastore_info["versions"][0] == {  # newest version must be the first element on the list
        "version": "0.2.0.0",
        "description": bump_desc,
        "releaseTime": pending_operations["releaseTime"],
        "languageCode": "no",
        "dataStructureUpdates": [
            {
                "dataSetName": "DATASET_B",
                "operation": "ADD",
                "description": "Dataset B",
                "releaseStatus": "RELEASED"
            }
        ]
    }

    # metadata_all__0_1_0.json file - unchanged
    metadata_all = datastore.get_metadata_all("0.1.0")
    assert len(metadata_all["dataStructures"]) == 1
    assert metadata_all["dataStructures"][0]["name"] == "DATASET_A"

    # metadata_all__0_2_0.json file - created
    metadata_all = datastore.get_metadata_all("0.2.0")
    assert len(metadata_all["dataStructures"]) == 2
    dataset_names = [
        data_structure["name"] for data_structure in metadata_all["dataStructures"]
    ]
    assert "DATASET_A" and "DATASET_B" in dataset_names

    # metadata_all_draft.json file - updated
    metadata_all_draft = datastore.get_metadata_all('draft')
    assert len(metadata_all_draft["dataStructures"]) == 2
    dataset_names = [
        data_structure["name"] for data_structure in metadata_all_draft["dataStructures"]
    ]
    assert "DATASET_A" and "DATASET_B" in dataset_names

    # Redundant metadata file DATASET_B__0_0_0.json - change in name
    assert not os.path.exists(datastore.create_metadata_file_path("DATASET_B", "0.0.0"))
    assert os.path.exists(datastore.create_metadata_file_path("DATASET_B", "0.2.0"))

    # Data file DATASET_B__0_0_0.parquet (or directory) - change in name
    assert not os.path.exists(datastore.create_data_file_path("DATASET_B", "0.0.0", False))
    assert os.path.exists(datastore.create_data_file_path("DATASET_B", "0.2.0", False))

    # data_versions__x_x_x.json - created
    data_versions = datastore.get_data_versions("0.2.0")
    assert len(data_versions) == 2
    assert data_versions["DATASET_A"] == "DATASET_A__0_1.parquet"
    assert data_versions["DATASET_B"] == "DATASET_B__0_2.parquet"
