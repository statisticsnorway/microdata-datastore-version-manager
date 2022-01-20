from datastore_version_manager.adapter import datastore
from datastore_version_manager.domain import metadata_all
from datastore_version_manager.util import semver, date
from datastore_version_manager.adapter.constants import (
    RELEASE_STATUS_ALLOWED_TRANSITIONS
)


def add_new(dataset_name: str, operation: str, release_status: str,
            description: str) -> None:
    pending_operations = datastore.get_pending_operations()
    data_structure_updates = pending_operations["dataStructureUpdates"]
    data_structure_updates.append({
        "name": dataset_name,
        "operation": operation,
        "description": description,
        "releaseStatus": release_status
    })

    pending_operations["releaseTime"] = date.seconds_since_epoch()
    pending_operations["version"] = semver.bump_draft_version(
        pending_operations["version"]
    )
    pending_operations["updateType"] = __get_update_type(
        data_structure_updates
    )
    __archive()
    datastore.write_pending_operations(pending_operations)
    metadata_all.generate_metadata_all_draft()


def remove(dataset_name: str):
    pending_operations = datastore.get_pending_operations()
    datastructure_updates = pending_operations["dataStructureUpdates"]
    dataset_in_pending_operations = any(
        dataset['name'] == dataset_name for dataset in datastructure_updates
    )
    if not dataset_in_pending_operations:
        raise datastore.DatasetNotFound(
            f'Dataset {dataset_name} not found in pending_operations.json'
        )
    for i in range(len(datastructure_updates)):
        if datastructure_updates[i]['name'] == dataset_name:
            del datastructure_updates[i]
            break
    pending_operations["updateType"] = __get_update_type(
        pending_operations["dataStructureUpdates"]
    )
    pending_operations["releaseTime"] = date.seconds_since_epoch()
    pending_operations["version"] = semver.bump_draft_version(
        pending_operations["version"]
    )
    __archive()
    datastore.write_pending_operations(pending_operations)
    metadata_all.generate_metadata_all_draft()


def set_release_status(dataset_name: str, release_status: str, operation: str,
                       description: str = None) -> None:
    pending_operations = datastore.get_pending_operations()
    pending_operations_list = pending_operations["dataStructureUpdates"]
    try:
        dataset = next(
            operation for operation in pending_operations_list
            if operation["name"] == dataset_name
        )
    except StopIteration:
        dataset = None

    if dataset:
        __check_if_transition_allowed(
            dataset["releaseStatus"], release_status
        )
        __archive()
        dataset["releaseStatus"] = release_status
        dataset["operation"] = operation

        if description:
            dataset["description"] = description

        pending_operations["releaseTime"] = date.seconds_since_epoch()
        pending_operations["version"] = semver.bump_draft_version(
            pending_operations["version"]
        )
        pending_operations["updateType"] = __get_update_type(
            pending_operations["dataStructureUpdates"]
        )
        datastore.write_pending_operations(pending_operations)
        metadata_all.generate_metadata_all_draft()
    else:
        if datastore.is_dataset_in_data_store(dataset_name, 'RELEASED'):
            __check_if_transition_allowed('RELEASED', release_status)
            add_new(
                dataset_name, operation, release_status, description
            )
        # dataset not found -> it needs to be ADDED first
        else:
            raise DatasetNotFound(
                f'Dataset {dataset_name} with status RELEASED '
                'not found in data_store'
            )


def get_release_status(dataset_name: str) -> str:
    pending_operations = datastore.get_pending_operations()
    data_structure_updates = pending_operations["dataStructureUpdates"]
    try:
        return next(
            data_structure["releaseStatus"]
            for data_structure in data_structure_updates
            if data_structure["name"] == dataset_name
        )
    except StopIteration:
        return None


def get_datastructure_updates() -> list:
    pending_operations = datastore.get_pending_operations()
    return pending_operations["dataStructureUpdates"]


def __check_if_transition_allowed(old_release_status, new_release_status):
    allowed_transitions = (
        RELEASE_STATUS_ALLOWED_TRANSITIONS[old_release_status]
    )
    if new_release_status not in allowed_transitions:
        raise ReleaseStatusTransitionNotAllowed(
            f'Transition from {old_release_status} to {new_release_status} '
            'is not allowed'
        )


def __archive() -> None:
    pending_operations = datastore.get_pending_operations()
    version = semver.dotted_to_underscored(
        pending_operations["version"]
    )
    file_path = (
        f'pending_operations/pending_operations__{version}.json'
    )
    datastore.write_to_archive(pending_operations, file_path)


def __get_update_type(data_structure_updates: list) -> str:
    operations = [
        data_structure["operation"]
        for data_structure in data_structure_updates
        if data_structure["releaseStatus"] in [
            "PENDING_RELEASE", "PENDING_DELETE"
        ]
    ]
    if not operations:
        return ""

    if "CHANGE_DATA" in operations or "REMOVE" in operations:
        return "MAJOR"
    elif "ADD" in operations:
        return "MINOR"
    elif "PATCH_METADATA":
        return "PATCH"
    else:
        raise InvalidOperation(
            f"Invalid operation in {operations}"
        )


class ReleaseStatusTransitionNotAllowed(Exception):
    pass


class InvalidOperation(Exception):
    pass


class DatasetNotFound(Exception):
    pass
