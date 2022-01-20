from datastore_version_manager.adapter import datastore
from datastore_version_manager.util import semver


def bump_datastore_info(description: str, pending_ops: dict, release_time: int, previous_version: str) -> tuple:
    data_structures_to_bump = []

    for data_structure in pending_ops["dataStructureUpdates"]:
        if data_structure["releaseStatus"] in ["PENDING_RELEASE", "PENDING_DELETE"]:
            data_structures_to_bump.append(data_structure.copy())

    if not data_structures_to_bump:
        raise Exception("Nothing to bump")

    new_version = __calculate_new_version__(data_structures_to_bump, previous_version)

    for data_structure in data_structures_to_bump:
        if data_structure["releaseStatus"] == "PENDING_RELEASE":
            data_structure["releaseStatus"] = "RELEASED"
        elif data_structure["releaseStatus"] == "PENDING_DELETE":
            data_structure["releaseStatus"] = "DELETED"
        else:
            raise Exception(f"Unknown releaseStatus {data_structure['releaseStatus']}")

    new_datastore_version = {
        "version": new_version,
        "description": description,
        "releaseTime": release_time,
        "languageCode": "no",
        "dataStructureUpdates": data_structures_to_bump
    }

    datastore_info = datastore.get_datastore_info()
    datastore_info["versions"].insert(0, new_datastore_version)
    datastore.write_datastore_info(datastore_info)

    return data_structures_to_bump, new_version


def __calculate_new_version__(data_structure_updates: list, previous_version: str) -> str:
    operations = [
        data_structure["operation"]
        for data_structure in data_structure_updates
        if data_structure["releaseStatus"] in [
            "PENDING_RELEASE", "PENDING_DELETE"
        ]
    ]

    if ("CHANGE_DATA" or "REMOVE") in operations:
        return semver.bump_major_version(previous_version)
    elif "ADD" in operations:
        return semver.bump_minor_version(previous_version)
    elif "PATCH_METADATA":
        return semver.bump_patch_version(previous_version)
    else:
        raise InvalidOperation(
            f"Invalid operation in {operations}"
        )