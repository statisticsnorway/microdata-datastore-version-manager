from datastore_version_manager.adapter import datastore
from datastore_version_manager.adapter.constants import (
    RELEASE_STATUS_ALLOWED_TRANSITIONS
)
from datastore_version_manager.domain import metadata_all
from datastore_version_manager.util import semver, date


def add_new(dataset_name: str, operation: str, release_status: str,
            description: str) -> None:
    pending_operations = datastore.get_pending_operations()
    data_structure_updates = pending_operations["dataStructureUpdates"]

    check_if_dataset_in_pending_operations(data_structure_updates, dataset_name)

    data_structure_updates.append({
        "name": dataset_name,
        "operation": operation,
        "description": description,
        "releaseStatus": release_status
    })

    pending_operations["releaseTime"] = date.seconds_since_epoch()
    pending_operations["version"] = f"0.0.0.{pending_operations['releaseTime']}"
    pending_operations["updateType"] = _get_update_type(
        data_structure_updates
    )
    _archive()
    datastore.write_pending_operations(pending_operations)
    metadata_all.generate_metadata_all_draft()


def check_if_dataset_in_pending_operations(data_structure_updates: dict, dataset_name: str):
    dataset_in_pending_operations = any(
        dataset['name'] == dataset_name for dataset in data_structure_updates
    )
    if dataset_in_pending_operations:
        raise PendingOperationException(
            f'Cannot add new pending operation for {dataset_name} because it already exists. '
            'Use update or hard delete.'
        )


def remove(dataset_name: str):
    pending_operations = datastore.get_pending_operations()
    datastructure_updates = pending_operations["dataStructureUpdates"]
    dataset_in_pending_operations = any(
        dataset['name'] == dataset_name for dataset in datastructure_updates
    )
    if not dataset_in_pending_operations:
        raise DatasetNotFound(
            f'Dataset {dataset_name} not found in pending_operations.json'
        )
    for i in range(len(datastructure_updates)):
        if datastructure_updates[i]['name'] == dataset_name:
            del datastructure_updates[i]
            break
    pending_operations["updateType"] = _get_update_type(
        pending_operations["dataStructureUpdates"]
    )
    pending_operations["releaseTime"] = date.seconds_since_epoch()
    pending_operations["version"] = f"0.0.0.{pending_operations['releaseTime']}"
    _archive()
    datastore.write_pending_operations(pending_operations)
    metadata_all.generate_metadata_all_draft()


def update_pending_operation(dataset_name: str, release_status: str, description: str = None) -> None:
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
        _check_if_transition_allowed(
            dataset["releaseStatus"], release_status
        )
        _archive()
        dataset["releaseStatus"] = release_status

        if description:
            dataset["description"] = description

        pending_operations["releaseTime"] = date.seconds_since_epoch()
        pending_operations["version"] = f"0.0.0.{pending_operations['releaseTime']}"
        pending_operations["updateType"] = _get_update_type(
            pending_operations["dataStructureUpdates"]
        )
        datastore.write_pending_operations(pending_operations)
        metadata_all.generate_metadata_all_draft()
    else:
        raise PendingOperationException(
            f'Cannot update pending operation for dataset {dataset_name}. '
            'Please add a new pending operation first.'
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


def _check_if_transition_allowed(old_release_status, new_release_status):
    allowed_transitions = (
        RELEASE_STATUS_ALLOWED_TRANSITIONS[old_release_status]
    )
    if new_release_status not in allowed_transitions:
        raise ReleaseStatusTransitionNotAllowed(
            f'Transition from {old_release_status} to {new_release_status} '
            'is not allowed'
        )


def _archive() -> None:
    pending_operations = datastore.get_pending_operations()
    version = semver.dotted_to_underscored(
        pending_operations["version"]
    )
    file_path = (
        f'pending_operations/pending_operations__{version}.json'
    )
    datastore.write_to_archive(pending_operations, file_path)


def _get_update_type(data_structure_updates: list) -> str:
    update_type, _ = semver.calculate_new_version(data_structure_updates)
    return update_type


class ReleaseStatusTransitionNotAllowed(Exception):
    pass


class DatasetNotFound(Exception):
    pass


class PendingOperationException(Exception):
    pass
