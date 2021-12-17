from datastore_version_manager.adapter import datastore
from datastore_version_manager.adapter.constants import RELEASE_STATUS_ALLOWED_TRANSITIONS
from datastore_version_manager.util import semver, date


def add_new(dataset_name: str, operation: str, release_status: str,
            description: str) -> None:
    pending_operations = datastore.get_pending_operations()
    pending_operations_list = pending_operations["dataStructureUpdates"]
    pending_operations_list.append({
        "name": dataset_name,
        "operation": operation,
        "description": description,
        "releaseStatus": release_status
    })

    pending_operations["releaseTime"] = date.seconds_since_epoch()
    pending_operations["version"] = semver.bump_draft_version(
        pending_operations["version"]
    )
    __archive()
    datastore.write_pending_operations(pending_operations)


def remove(dataset_name: str):
    pending_operations = datastore.get_pending_operations()
    datastructure_updates = pending_operations["dataStructureUpdates"]
    if not any(dataset['name'] == dataset_name for dataset in datastructure_updates):
        raise datastore.DatasetNotFound(
            f'Dataset {dataset_name} not found in pending_operations.json'
        )
    for i in range(len(datastructure_updates)):
        if datastructure_updates[i]['name'] == dataset_name:
            del datastructure_updates[i]
            break
    __archive()
    pending_operations["releaseTime"] = date.seconds_since_epoch()
    datastore.write_pending_operations(pending_operations)


def set_release_status(dataset_name: str, release_status: str, operation: str,
                       description: str = None) -> None:
    pending_operations = datastore.get_pending_operations()
    pending_operations_list = pending_operations["dataStructureUpdates"]
    try:
        dataset_on_pending_operations_list = next(
            operation for operation in pending_operations_list
            if operation["name"] == dataset_name
        )
    except StopIteration:
        dataset_on_pending_operations_list = None

    if dataset_on_pending_operations_list:
        __check_if_transition_allowed(
            dataset_on_pending_operations_list["releaseStatus"], release_status
        )
        __archive()
        dataset_on_pending_operations_list["releaseStatus"] = release_status
        dataset_on_pending_operations_list["operation"] = operation
        dataset_on_pending_operations_list["description"] = description

        pending_operations["releaseTime"] = date.seconds_since_epoch()
        pending_operations["version"] = semver.bump_draft_version(
            pending_operations["version"]
        )
        datastore.write_pending_operations(pending_operations)
    else:
        if datastore.is_dataset_in_data_store(dataset_name, 'RELEASED'):
            __check_if_transition_allowed('RELEASED', release_status)
            add_new(
                dataset_name, operation, release_status, description
            )
        # dataset not found -> it needs to be ADDED first
        else:
            raise DatasetNotFound(
                f'Dataset {dataset_name} with status RELEASED not found in data_store'
            )


def __check_if_transition_allowed(old_release_status, new_release_status):
    if new_release_status not in RELEASE_STATUS_ALLOWED_TRANSITIONS[old_release_status]:
        raise ReleaseStatusTransitionNotAllowed(
            f'Transition from {old_release_status} to {new_release_status} is not allowed'
        )


def __archive() -> None:
    pending_operations = datastore.get_pending_operations()
    version = pending_operations["version"]
    file_path = (
        f'pending_operations/pending_operation__{version.replace(".", "_")}'
    )
    datastore.write_to_archive(pending_operations, file_path)


class ReleaseStatusTransitionNotAllowed(Exception):
    pass


class DatasetNotFound(Exception):
    pass
