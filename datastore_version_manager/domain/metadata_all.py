import json

from datastore_version_manager.adapter import datastore
from datastore_version_manager.domain import pending_operations


def generate_metadata_all_draft():
    data_structure_updates = pending_operations.get_datastructure_updates()
    latest_version = datastore.get_latest_version()
    latest_metadata_all = datastore.get_metadata_all(latest_version[:5])
    data_structures = latest_metadata_all['dataStructures']

    for data_structure_update in data_structure_updates:
        dataset_name = data_structure_update['name']
        if data_structure_update['releaseStatus'] == 'PENDING_DELETE':
            data_structures = [
                data_structure for data_structure in data_structures
                if data_structure['name'] != dataset_name
            ]
        else:
            draft_metadata_file_path = datastore.create_metadata_file_path(
                dataset_name, "0.0.0"
            )
            with open(draft_metadata_file_path, 'r') as f:
                draft_metadata = json.load(f)
            data_structures.append(draft_metadata)
    datastore.write_metadata_all_draft({
        "dataStore": latest_metadata_all["dataStore"],
        "dataStructures": data_structures
    })


def generate_versioned_metadata_all():
    pass
