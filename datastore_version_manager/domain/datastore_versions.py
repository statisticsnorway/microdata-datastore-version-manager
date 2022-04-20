from datastore_version_manager.adapter import datastore
from datastore_version_manager.util import semver


def bump_datastore_versions(description: str, pending_ops: dict, release_time: int, previous_version: str) -> tuple:
    data_structures_to_bump = []

    for data_structure in pending_ops["dataStructureUpdates"]:
        if data_structure["releaseStatus"] in ["PENDING_RELEASE", "PENDING_DELETE"]:
            data_structures_to_bump.append(data_structure.copy())

    if not data_structures_to_bump:
        raise NothingToBump("Nothing to bump")

    _, new_version = semver.calculate_new_version(data_structures_to_bump, previous_version)

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

    datastore_versions = datastore.get_datastore_versions()
    datastore_versions["versions"].insert(0, new_datastore_version)
    datastore.write_datastore_versions(datastore_versions)

    return data_structures_to_bump, new_version


class NothingToBump(Exception):
    pass