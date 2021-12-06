from typing import Optional

from datastore_version_manager.adapter import datastore
from datastore_version_manager.adapter.constants import RELEASE_STATUS_ALLOWED_TRANSITIONS
from datastore_version_manager.util import semver


def add_new_pending_operation(dataset_name: str, operation: str, release_status: str,
                              description: str) -> None:
    pending_operations = datastore.get_pending_operations()
    pending_operations_list = pending_operations["pendingOperations"]
    pending_operations_list.append({
        "datasetName": dataset_name,
        "operation": operation,
        "description": description,
        "releaseStatus": release_status
    })
    pending_operations["version"] = semver.bump_draft_version(
        pending_operations["version"]
    )
    datastore.write_pending_operations(pending_operations)


def set_release_status(dataset_name: str, release_status: str, operation: str,
                       description: str = None) -> None:

    pending_operations = datastore.get_pending_operations()
    pending_operations_list = pending_operations["pendingOperations"]
    try:
        dataset_on_pending_operations_list = next(
            operation for operation in pending_operations_list
            if operation["datasetName"] == dataset_name
        )
    except StopIteration:
        dataset_on_pending_operations_list = None

    if dataset_on_pending_operations_list:
        check_if_transition_allowed(dataset_on_pending_operations_list["releaseStatus"], release_status)
        dataset_on_pending_operations_list["releaseStatus"] = release_status
        pending_operations["version"] = semver.bump_draft_version(
            pending_operations["version"]
        )
        datastore.write_pending_operations(pending_operations)
    else:
        datastore.find_latest_in_metadata_all(dataset_name)
        # dataset found -> its release status is RELEASED
        # dataset not found -> it needs to be ADDED first
        check_if_transition_allowed('RELEASED', release_status)
        add_new_pending_operation(
            dataset_name, operation, release_status, description
        )


def check_if_transition_allowed(old_release_status, new_release_status):
    if new_release_status not in RELEASE_STATUS_ALLOWED_TRANSITIONS[old_release_status]:
        raise ReleaseStatusTransitionNotAllowed(
            f'Transition from {old_release_status} to {new_release_status} is not allowed'
        )


class ReleaseStatusTransitionNotAllowed(Exception):
    pass
