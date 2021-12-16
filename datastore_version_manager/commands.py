import shutil

from datastore_version_manager.adapter import (
    datastore, built_datasets
)
from datastore_version_manager.adapter.constants import (
    USER_CHANGEABLE_RELEASE_STATUSES
)
from datastore_version_manager.domain import pending_operations


def add_new_dataset(dataset_name: str, description: str, overwrite: bool):
    if datastore.is_dataset_in_data_store(dataset_name, "RELEASED"):
        raise ForbiddenOperation(
            f'Can not add new variable "{dataset_name}". '
            'A versioned variable of the same name already exists in datastore'
        )

    release_status = pending_operations.get_release_status(dataset_name)

    if release_status == 'DRAFT' and overwrite:
        datastore.delete_draft_dataset(dataset_name)
    elif release_status is not None:
        raise ForbiddenOperation(
            f'Can not add new variable "{dataset_name}"'
            'It exists in the draft version of datastore with '
            f'release status: {release_status}'
        )

    built_data_path = built_datasets.get_data_path(dataset_name)
    if '.parquet' in built_data_path:
        draft_data_path = (
            f'{datastore.get_data_dir_path(dataset_name)}/'
            f'{dataset_name}__0_0.parquet'
        )
    else:
        draft_data_path = (
            f'{datastore.get_data_dir_path(dataset_name)}/'
            f'{dataset_name}__0_0'
        )
    built_metadata_path = built_datasets.get_metadata_path(dataset_name)
    draft_metadata_path = (
        f'{datastore.get_metadata_dir_path(dataset_name)}/'
        f'{dataset_name}__0_0_0.json'
    )
    shutil.move(built_metadata_path, draft_metadata_path)
    shutil.move(built_data_path, draft_data_path)
    pending_operations.add_new(dataset_name, "ADD", "DRAFT", description)


def set_status(dataset_name: str, release_status: str, operation: str = None,
               description: str = None):
    if release_status not in USER_CHANGEABLE_RELEASE_STATUSES['MUTABLE']:
        raise NoSuchReleaseStatus(
            f'release status must be one of '
            f'{USER_CHANGEABLE_RELEASE_STATUSES["MUTABLE"]}'
        )
    pending_operations.set_release_status(
        dataset_name, release_status, operation, description
    )


def hard_delete(dataset_name: str):
    pending_operations.remove(dataset_name)
    datastore.delete_draft_dataset(dataset_name)


class NoSuchReleaseStatus(Exception):
    pass


class ForbiddenOperation(Exception):
    pass
