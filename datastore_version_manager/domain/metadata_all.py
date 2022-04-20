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
        elif data_structure_update['operation'] == 'ADD':
            draft_metadata = datastore.get_metadata(dataset_name, "0.0.0")
            data_structures.append(draft_metadata)
        else:
            # remove previous data_structure and update with new one
            data_structures = [
                data_structure for data_structure in data_structures
                if data_structure['name'] != dataset_name
            ]
            draft_metadata = datastore.get_metadata(dataset_name, "0.0.0")
            data_structures.append(draft_metadata)

    datastore.write_metadata_all(
        {
            "dataStore": latest_metadata_all["dataStore"],
            "dataStructures": data_structures
        },
        "draft")


def create_new_version(bumped_data_structures: dict, previous_version: str, new_version: str) -> None:
    if previous_version == "0.0.0":
        # no previous metadata_all__x_x_x.json
        # TODO: make this more generic
        metadata_all = {
            "dataStore": {
                "name": "no.ssb.fdb",
                "label": "Data fra SSB",
                "description": "Registerdata som inngår i SSBs statistikkproduksjon",
                "languageCode": "no"
            },
            "dataStructures": []
        }
    else:
        metadata_all = datastore.get_metadata_all(previous_version)

        # remove DELETED datasets from the new metadata_all
        deleted_structures = [
            data_structure["name"] for data_structure in bumped_data_structures
            if data_structure["releaseStatus"] == "DELETED"
        ]
        metadata_all["dataStructures"] = [
            ds for ds in metadata_all["dataStructures"]
            if ds["name"] not in deleted_structures
        ]

    for data_structure in bumped_data_structures:
        if data_structure["releaseStatus"] == "RELEASED":
            # If the dataset is already on the list then update, don't append
            found = False
            for i, ds in enumerate(metadata_all["dataStructures"]):
                if ds["name"] == data_structure["name"]:
                    metadata_all["dataStructures"][i] = data_structure
                    found = True
                    break

            if not found:
                metadata_all["dataStructures"].append(
                    datastore.get_metadata(data_structure["name"], "0.0.0")
                )

    datastore.write_metadata_all(metadata_all, new_version)
