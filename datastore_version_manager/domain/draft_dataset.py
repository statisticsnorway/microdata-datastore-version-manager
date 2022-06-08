import shutil

from microdata_validator import Metadata

from datastore_version_manager.adapter import (
    datastore, built_datasets
)
from datastore_version_manager.adapter.constants import (
    USER_CHANGEABLE_RELEASE_STATUSES
)
from datastore_version_manager.domain import pending_operations
from datastore_version_manager.exceptions.exceptions import (
    ForbiddenOperation, NoSuchReleaseStatus
)


def add_new_draft_dataset(operation_type: str,
                          dataset_name: str, description: str) -> None:
    _check_if_dataset_was_deleted(dataset_name)

    if operation_type == 'ADD_OR_CHANGE_DATA':
        if datastore.is_dataset_in_datastore_versions(dataset_name, "RELEASED"):
            operation_type = 'CHANGE_DATA'
        else:
            operation_type = 'ADD'

    if operation_type == 'PATCH_METADATA':
        if not datastore.is_dataset_in_datastore_versions(dataset_name, "RELEASED"):
            raise ForbiddenOperation(
                f'Cannot patch variable "{dataset_name}". '
                'This variable has not been released.'
            )

    release_status = pending_operations.get_release_status(dataset_name)

    if release_status == 'DRAFT':
        datastore.delete_draft_dataset(dataset_name)
    elif release_status is not None:
        raise ForbiddenOperation(
            f'Cannot add or update variable "{dataset_name}" '
            'It exists in the draft version of datastore with '
            f'release status: {release_status} '
            f'Please use hard delete first'
        )

    if operation_type == 'ADD' or operation_type == 'CHANGE_DATA':
        built_data_path = (
            built_datasets.get_data_path(dataset_name)
        )
        datastore.create_data_dir_path(dataset_name)
        draft_data_path = datastore.get_data_file_path(
            dataset_name, "DRAFT"
        )
        shutil.move(built_data_path, draft_data_path)

    built_metadata_path = built_datasets.get_metadata_path(dataset_name)
    draft_metadata_path = datastore.get_metadata_file_path(
        dataset_name, "DRAFT"
    )

    if operation_type == 'PATCH_METADATA':
        _patch_metadata(built_metadata_path, dataset_name)

    shutil.move(built_metadata_path, draft_metadata_path)

    pending_operations.add_new(dataset_name, operation_type, "DRAFT", description)


def _patch_metadata(built_metadata_path: str, dataset_name: str):
    latest_version = datastore.get_latest_version()
    latest_metadata = datastore.get_metadata(
        dataset_name, latest_version
    )
    built_metadata = built_datasets.get_metadata(dataset_name)
    latest = Metadata(latest_metadata)
    latest.patch(Metadata(built_metadata))
    patched = latest.to_dict()
    built_datasets.write_metadata(patched, built_metadata_path)


def _check_if_dataset_was_deleted(dataset_name):
    if datastore.is_dataset_in_datastore_versions(dataset_name, "DELETED"):
        raise ForbiddenOperation(
            f'Cannot add or update variable "{dataset_name}". '
            'This variable has been removed from datastore'
        )


def update_pending_operation(dataset_name: str, release_status: str, description: str = None):
    if release_status not in USER_CHANGEABLE_RELEASE_STATUSES['MUTABLE']:
        raise NoSuchReleaseStatus(
            f'release status must be one of '
            f'{USER_CHANGEABLE_RELEASE_STATUSES["MUTABLE"]}'
        )
    pending_operations.update_pending_operation(
        dataset_name, release_status, description
    )


def hard_delete(dataset_name: str):
    pending_operations.remove(dataset_name)
    datastore.delete_draft_dataset(dataset_name)
