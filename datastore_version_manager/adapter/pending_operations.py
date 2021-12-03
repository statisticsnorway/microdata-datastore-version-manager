from datastore_version_manager.adapter import datastore
from datastore_version_manager.adapter.constants import RELEASE_STATUS_ALLOWED_TRANSITIONS
from datastore_version_manager.util import semver


def add_new_draft(dataset_name: str, operation: str,
                  description: str) -> None:
    pending_operations = datastore.get_pending_operations()
    pending_operations_list = pending_operations["pendingOperations"]
    pending_operations_list.append({
        "datasetName" : dataset_name,
        "operation" : operation,
        "description" : description,
        "releaseStatus": "DRAFT"
    })
    pending_operations["version"] = semver.bump_draft_version(
        pending_operations["version"]
    )
    datastore.write_pending_operations(pending_operations)


def update_release_status(dataset_name: str, release_status: str) -> None:
    pending_operations = datastore.get_pending_operations()
    pending_operations_list = pending_operations["pendingOperations"]
    try:
        dataset = next(
            operation for operation in pending_operations_list
            if operation["datasetName"] == dataset_name
        )

        if release_status not in RELEASE_STATUS_ALLOWED_TRANSITIONS[dataset["releaseStatus"]]:
            raise ReleaseStatusTransitionNotAllowed(
                f'Transition from {dataset["releaseStatus"]} to {release_status} is not allowed'
            )

        dataset["releaseStatus"] = release_status
        pending_operations["version"] = semver.bump_draft_version(
            pending_operations["version"]
        )
        datastore.write_pending_operations(pending_operations)
    except StopIteration:
        raise NoSuchPendingOperation(
            f'Unable to change release status of {dataset_name}.'
            ' {dataset_name} not in list of pending operations'
        )


class NoSuchPendingOperation(Exception):
    pass


class ReleaseStatusTransitionNotAllowed(Exception):
    pass
