from datastore_version_manager.adapter import datastore
from datastore_version_manager.domain import pending_operations, metadata_all, data_versions, datastore_versions
from datastore_version_manager.util import date


def bump_version(description: str):
    release_time = date.seconds_since_epoch()
    pending_ops = datastore.get_pending_operations()
    previous_version = datastore.get_latest_version()
    pre_bump_pending_operations = datastore.get_pending_operations()

    bumped_data_structures, new_version = datastore_versions.bump_datastore_versions(
        description, pending_ops, release_time, previous_version
    )

    metadata_all.create_new_version(bumped_data_structures, previous_version, new_version)

    _update_pending_operations(pending_ops)

    _change_draft_metadata_file_names(bumped_data_structures, new_version)

    _change_draft_data_file_names(bumped_data_structures, new_version)

    data_versions.create_new_version(bumped_data_structures, previous_version, new_version)

    datastore.remove_archived_pending_operations(pre_bump_pending_operations, new_version)


def _change_draft_metadata_file_names(bumped_data_structures: dict, new_version: str) -> None:
    """
    Change metadata file names of data structures that were RELEASED
    from <dataset>__DRAFT.json to <dataset>__<new_version>.json.
    """
    for data_structure in bumped_data_structures:
        if data_structure["releaseStatus"] == "RELEASED":
            datastore.change_draft_metadata_file_name(
                data_structure["name"], new_version
            )


def _change_draft_data_file_names(bumped_data_structures: dict,
                                  new_version: str) -> None:
    """
    Change data file names of data structures that were RELEASED
    from <dataset>__DRAFT(.parquet) to <dataset>__<new_version>(.parquet).
    """
    for data_structure in bumped_data_structures:
        if data_structure["releaseStatus"] == "RELEASED":
            datastore.change_draft_data_file_name(
                data_structure["name"], new_version
            )


def _update_pending_operations(pending_ops):
    """
     Remove data structures that were bumped. It will also update
     metadata_all_draft as the two need to be in sync.
    """
    for data_structure in pending_ops["dataStructureUpdates"]:
        if data_structure["releaseStatus"] in ["PENDING_RELEASE", "PENDING_DELETE"]:
            pending_operations.remove(data_structure["name"])


def get_bump_manifesto():
    datastructure_updates = pending_operations.get_datastructure_updates()

    filtered = [
        data_structure for data_structure in datastructure_updates
        if data_structure['releaseStatus'] == 'PENDING_RELEASE'
        or data_structure['releaseStatus'] == 'PENDING_DELETE'
    ]

    return filtered


def apply_bump_manifesto(client_bump_manifesto: list[dict], description: str):
    current_bump_manifesto = get_bump_manifesto()
    if not client_bump_manifesto == current_bump_manifesto:
        raise BumpManifestoOutOfDate(
            f'Bump manifesto has changed. Please retrieve it again.'
        )
    else:
        # There is a very small chance that pending operations could be changed
        # after getting current_bump_manifesto and before bump_version.
        # Alternatively pending_ops_from_client could be used as parameter to bump_version
        # and bump_version function must then not read the pending_operations.json file.
        bump_version(description)


class BumpManifestoOutOfDate(Exception):
    pass
