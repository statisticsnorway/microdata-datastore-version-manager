import os
import shutil

import pytest

from datastore_version_manager.domain import version_bumper, draft_dataset
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

    assert len(datastore.get_archive('pending_operations')) > 0

    bump_desc = "First version"
    version_bumper.bump_version(bump_desc)

    # datastore_versions.json file - updated
    datastore_versions = datastore.get_datastore_versions()
    assert len(datastore_versions["versions"]) == 1
    assert datastore_versions["versions"][0]["version"] == "0.1.0.0"
    assert datastore_versions["versions"][0]["description"] == bump_desc
    assert datastore_versions["versions"][0]["releaseTime"] > previous_release_time
    assert datastore_versions["versions"][0]["dataStructureUpdates"][0] == \
           {
               "name": "DATASET_A",
               "operation": "ADD",
               "description": "Dataset A",
               "releaseStatus": "RELEASED"
           }

    # pending_operations.json file - updated
    pending_operations = datastore.get_pending_operations()
    assert pending_operations["version"] == f"0.0.0.{pending_operations['releaseTime']}"
    assert pending_operations["releaseTime"] > previous_release_time
    assert pending_operations["updateType"] == ""
    assert pending_operations["dataStructureUpdates"][0] == {
        "name": "DATASET_B",
        "operation": "ADD",
        "description": "Dataset B",
        "releaseStatus": "DRAFT"
    }

    # metadata_all__1_0_0_0.json file - created
    metadata_all = datastore.get_metadata_all("0.1.0.0")
    assert len(metadata_all["dataStructures"]) == 1
    assert metadata_all["dataStructures"][0]["name"] == "DATASET_A"
    assert "identifierVariables" in metadata_all["dataStructures"][0]

    # metadata_all_draft.json file - not changed due to the setup of this test case
    metadata_all_draft = datastore.get_metadata_all('DRAFT')
    assert len(metadata_all_draft["dataStructures"]) == 2
    dataset_names = [
        data_structure["name"] for data_structure in metadata_all_draft["dataStructures"]
    ]
    assert ("DATASET_A" and "DATASET_B") in dataset_names

    # Redundant metadata file DATASET_A__DRAFT.json - change in name
    assert not os.path.exists(datastore.get_metadata_file_path("DATASET_A", "DRAFT"))
    assert os.path.exists(datastore.get_metadata_file_path("DATASET_A", "0.1.0.0"))

    # Redundant metadata file DATASET_B__DRAFT.json - unchanged
    assert os.path.exists(datastore.get_metadata_file_path("DATASET_B", "DRAFT"))

    # Data file DATASET_A__DRAFT.parquet - change in name
    assert not os.path.exists(datastore.get_data_file_path("DATASET_A", "DRAFT"))
    assert os.path.exists(datastore.get_data_file_path("DATASET_A", "0.1.0.0"))

    # Data dir DATASET_B__0_0 - unchanged
    assert os.path.exists(datastore.get_data_file_path("DATASET_B", "DRAFT"))

    # data_versions__0_1_0.0.json - created
    data_versions = datastore.get_data_versions("0.1.0.0")
    assert len(data_versions) == 1
    assert data_versions["DATASET_A"] == "DATASET_A__0_1.parquet"

    assert len(datastore.get_archive('pending_operations')) == 0


def test_bump_version_twice():
    bump_desc = "First version"
    version_bumper.bump_version(bump_desc)

    draft_dataset.update_pending_operation("DATASET_B", "PENDING_RELEASE")

    assert len(datastore.get_archive('pending_operations')) == 1

    pending_operations = datastore.get_pending_operations()
    assert pending_operations["version"] == f"0.0.0.{pending_operations['releaseTime']}"
    assert pending_operations["dataStructureUpdates"][0] == {
        "name": "DATASET_B",
        "operation": "ADD",
        "description": "Dataset B",
        "releaseStatus": "PENDING_RELEASE"
    }

    bump_desc = "Second version"
    version_bumper.bump_version(bump_desc)

    # pending_operations.json file - updated
    pending_operations = datastore.get_pending_operations()
    assert pending_operations["version"] == f"0.0.0.{pending_operations['releaseTime']}"
    assert pending_operations["updateType"] == ""  # no datasets
    assert len(pending_operations["dataStructureUpdates"]) == 0

    # datastore_versions.json file - updated
    datastore_versions = datastore.get_datastore_versions()
    assert len(datastore_versions["versions"]) == 2
    # newest version must be the first element on the list
    assert datastore_versions["versions"][0] == {
        "version": "0.2.0.0",
        "description": bump_desc,
        "releaseTime": pending_operations["releaseTime"],
        "languageCode": "no",
        "dataStructureUpdates": [
            {
                "name": "DATASET_B",
                "operation": "ADD",
                "description": "Dataset B",
                "releaseStatus": "RELEASED"
            }
        ]
    }

    # metadata_all__0_1_0_0.json file - unchanged
    metadata_all = datastore.get_metadata_all("0.1.0.0")
    assert len(metadata_all["dataStructures"]) == 1
    assert metadata_all["dataStructures"][0]["name"] == "DATASET_A"

    # metadata_all__0_2_0.json file - created
    metadata_all = datastore.get_metadata_all("0.2.0.0")
    assert len(metadata_all["dataStructures"]) == 2
    dataset_names = [
        data_structure["name"] for data_structure in metadata_all["dataStructures"]
    ]
    assert "DATASET_A" and "DATASET_B" in dataset_names

    # metadata_all_draft.json file - updated
    metadata_all_draft = datastore.get_metadata_all('DRAFT')
    assert len(metadata_all_draft["dataStructures"]) == 2
    dataset_names = [
        data_structure["name"] for data_structure in metadata_all_draft["dataStructures"]
    ]
    assert "DATASET_A" and "DATASET_B" in dataset_names

    # Redundant metadata file DATASET_A__0_1_0.0.json - unchanged
    assert os.path.exists(datastore.get_metadata_file_path("DATASET_A", "0.1.0.0"))
    assert not os.path.exists(datastore.get_metadata_file_path("DATASET_A", "0.2.0.0"))

    # Redundant metadata file DATASET_B__DRAFT.json - change in name
    assert not os.path.exists(datastore.get_metadata_file_path("DATASET_B", "DRAFT"))
    assert os.path.exists(datastore.get_metadata_file_path("DATASET_B", "0.2.0.0"))

    # Data file DATASET_A__0_1.parquet - unchanged
    assert os.path.exists(datastore.get_data_file_path("DATASET_A", "0.1.0.0"))
    assert not os.path.exists(datastore.get_data_file_path("DATASET_A", "0.2.0.0"))

    # Data dir DATASET_B__0_0 - change in name
    assert not os.path.exists(datastore.get_data_file_path("DATASET_B", "DRAFT"))
    assert os.path.exists(datastore.get_data_file_path("DATASET_B", "0.2.0.0"))

    # data_versions__0_2_0.json - created
    data_versions = datastore.get_data_versions("0.2.0.0")
    assert len(data_versions) == 2
    assert data_versions["DATASET_A"] == "DATASET_A__0_1.parquet"
    assert data_versions["DATASET_B"] == "DATASET_B__0_2"

    # data_versions__0_1_0_0.json - unchanged
    data_versions = datastore.get_data_versions("0.1.0.0")
    assert len(data_versions) == 1
    assert data_versions["DATASET_A"] == "DATASET_A__0_1.parquet"

    assert len(datastore.get_archive('pending_operations')) == 0


def test_get_bump_manifesto():
    bump_manifesto = version_bumper.get_bump_manifesto()

    assert len(bump_manifesto) == 1
    assert {
               "name": "DATASET_A",
               "description": "Dataset A",
               "operation": "ADD",
               "releaseStatus": "PENDING_RELEASE"
           } in bump_manifesto
