import shutil

from datastore_version_manager.adapter import datastore, built_datasets
from datastore_version_manager.util import util


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
    pending_operations = datastore.get_pending_operations_json()
    pending_operations_list = pending_operations["pendingOperations"]
    pending_operations_list.append({
        "datasetName" : dataset_name,
        "operation" : operation,
        "description" : description,
        "releaseStatus": "DRAFT"
    })
    pending_operations["version"] = util.bump_draftpatch(
        pending_operations["version"]
    )
    datastore.write_pending_operations_json(pending_operations)


def set_status_for_pending_operation(dataset_name: str, status_message: str):
    if status_message not in ["PENDING_RELEASE", "DRAFT"]:
        raise noSuchReleaseStatus(
            'release status must be one of ["PENDING_RELEASE", "DRAFT"]'
        )
    pending_operations = datastore.get_pending_operations_json()
    pending_operations_list = pending_operations["pendingOperations"]
    try:
        dataset = next(
            operation for operation in pending_operations_list
            if operation["datasetName"] == dataset_name
        )
        dataset["releaseStatus"] = "PENDING_RELEASE"
        pending_operations["version"] = util.bump_draftpatch(
            pending_operations["version"]
        )
        datastore.write_pending_operations_json(pending_operations)
    except StopIteration:
        raise noSuchPendingOperation(
            f'Unable to change release status of {dataset_name}.'
            ' {dataset_name} not in list of pending operations'
        )


class noSuchPendingOperation(Exception):
    pass


class noSuchReleaseStatus(Exception):
    pass
