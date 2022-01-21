from datastore_version_manager.adapter import datastore
from datastore_version_manager.util import date, semver
from datastore_version_manager.domain import pending_operations, metadata_all, datastore_info, data_versions


def bump_version(description: str):
    release_time = date.seconds_since_epoch()
    pending_ops = datastore.get_pending_operations()
    previous_version = datastore.get_latest_version()
    pre_bump_pending_operations = datastore.get_pending_operations()

    bumped_data_structures, new_version = datastore_info.bump_datastore_info(
        description, pending_ops, release_time, previous_version
    )

    metadata_all.create_new_version(bumped_data_structures, previous_version, new_version)

    __update_pending_operations__(pending_ops)

    __change_metadata_file_names__(bumped_data_structures, new_version)

    __change_data_file_names__(bumped_data_structures, new_version)

    data_versions.create_new_version(bumped_data_structures, previous_version, new_version)

    datastore.remove_archived_pending_operations(pre_bump_pending_operations, new_version)


def __change_metadata_file_names__(bumped_data_structures: dict, new_version: str) -> None:
    """
    Change metadata file names of data structures that were RELEASED
    from <dataset>__0_0_0.json to <dataset>__<new_version>.json.
    """
    for data_structure in bumped_data_structures:
        if data_structure["releaseStatus"] == "RELEASED":
            datastore.change_metadata_file_name(data_structure["name"], new_version)


def __change_data_file_names__(bumped_data_structures: dict, new_version: str) -> None:
    """
    Change data file names of data structures that were RELEASED
    from <dataset>__0_0(.parquet) to <dataset>__<new_version>(.parquet).
    """
    for data_structure in bumped_data_structures:
        if data_structure["releaseStatus"] == "RELEASED":
            datastore.change_data_file_name(data_structure["name"], new_version)


def __update_pending_operations__(pending_ops):
    """
     Remove data structures that were bumped. It will also update metadata_all_draft as the two need to be in sync.
    """
    for data_structure in pending_ops["dataStructureUpdates"]:
        if data_structure["releaseStatus"] in ["PENDING_RELEASE", "PENDING_DELETE"]:
            pending_operations.remove(data_structure["name"])
