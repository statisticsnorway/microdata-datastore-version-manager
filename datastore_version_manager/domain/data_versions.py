from datastore_version_manager.adapter import datastore


def create_new_version(bumped_data_structures: dict, previous_version: str, new_version: str) -> None:
    if previous_version == "0.0.0":
        # no previous data_versions__x_x_x.json
        data_versions = {}
    else:
        data_versions = datastore.get_data_versions(previous_version)

        # remove DELETED datasets from the new data_versions
        deleted_structures = [
            data_structure["name"] for data_structure in bumped_data_structures
            if data_structure["releaseStatus"] == "DELETED"
        ]
        data_versions = {
            name: path for name, path in data_versions.items()
            if name not in deleted_structures
        }

    for data_structure in bumped_data_structures:
        if data_structure["releaseStatus"] == "RELEASED":
            data_file_path = datastore.get_data_file_path(data_structure["name"], new_version)
            data_versions[data_structure["name"]] = data_file_path.split("/")[-1]

    datastore.write_data_versions(data_versions, new_version)
