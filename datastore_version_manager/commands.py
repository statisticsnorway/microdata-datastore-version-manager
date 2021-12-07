import shutil

from datastore_version_manager.adapter import (
    datastore, built_datasets
)
from datastore_version_manager.adapter.constants import RELEASE_STATUSES
from datastore_version_manager.domain import pending_operations


def new_draft_to_datastore(dataset_name: str, description: str,
                           operation: str):
    built_dataset_path = built_datasets.get_dataset_path(dataset_name)
    if not datastore.dataset_exists(dataset_name):
        datastore.new_dataset_directory(dataset_name)
    shutil.move(
        f'{built_dataset_path}/{dataset_name}__0_0_0.json',
        f'{datastore.get_metadata_dir_path(dataset_name)}/{dataset_name}__0_0_0.json'
    )
    shutil.move(
        f'{built_dataset_path}/{dataset_name}__0_0.parquet',
        f'{datastore.get_data_dir_path(dataset_name)}/{dataset_name}__0_0.parquet'
    )
    pending_operations.add_new_pending_operation(dataset_name, operation, "DRAFT", description)


def set_status(dataset_name: str, release_status: str, operation: str = None, description: str = None):
    if release_status not in RELEASE_STATUSES['MUTABLE']:
        raise NoSuchReleaseStatus(
            f'release status must be one of {RELEASE_STATUSES["MUTABLE"]}'
        )
    pending_operations.set_release_status(dataset_name, release_status, operation, description)


class NoSuchReleaseStatus(Exception):
    pass
