import json
import os
import shutil
from datastore_version_manager.util import semver


def get_pending_operations() -> dict:
    pending_operations_json = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/pending_operations.json'
    )
    with open(pending_operations_json, encoding="utf-8") as f:
        return json.load(f)


def write_pending_operations(pending_operation: dict):
    pending_operations_json = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/pending_operations.json'
    )
    with open(pending_operations_json, 'w', encoding="utf-8") as f:
        json.dump(pending_operation, f, indent=4)


def get_datastore_info() -> dict:
    data_store_json = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/data_store.json'
    )
    with open(data_store_json, encoding="utf-8") as f:
        return json.load(f)


def write_datastore_info(datastore_dict: dict):
    data_store_json = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/data_store.json'
    )
    with open(data_store_json, 'w', encoding="utf-8") as f:
        return json.dump(datastore_dict, f, indent=4)


def write_to_archive(json_dict: dict, file_path: str):
    file_path = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/archive/{file_path}'
    )
    with open(file_path, 'w', encoding="utf-8") as f:
        json.dump(json_dict, f, indent=4)


def dataset_exists(dataset_name: str):
    data_dir_exists = os.path.isdir(
        f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}'
    )
    metadata_dir_exists = os.path.isdir(
        f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}'
    )
    return data_dir_exists and metadata_dir_exists


def draft_dataset_exists(dataset_name: str):
    pending_operations = get_pending_operations()
    datasets = pending_operations["dataStructureUpdates"]
    dataset_list = [
        dataset for dataset in datasets if dataset['name'] == dataset_name
    ]
    return len(dataset_list) > 0


def delete_draft_dataset(dataset_name: str):
    metadata_dir = get_metadata_dir_path(dataset_name)
    os.remove(f'{metadata_dir}/{dataset_name}__0_0_0.json')
    if len(os.listdir(metadata_dir)) == 0:
        shutil.rmtree(metadata_dir)

    data_dir = get_data_dir_path(dataset_name)
    single_parquet = f'{data_dir}/{dataset_name}__0_0.parquet'
    if os.path.exists(single_parquet):
        os.remove(single_parquet)
    else:
        partitioned_parquet = f'{data_dir}/{dataset_name}__0_0'
        shutil.rmtree(partitioned_parquet)

    if len(os.listdir(data_dir)) == 0:
        shutil.rmtree(data_dir)


def new_dataset_directory(dataset_name: str) -> None:
    os.mkdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}')
    os.mkdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}')


def get_metadata_dir_path(dataset_name: str) -> str:
    metadata_dir_path = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}'
    )
    if not os.path.isdir(metadata_dir_path):
        os.mkdir(metadata_dir_path)
    return metadata_dir_path


def get_data_dir_path(dataset_name: str) -> str:
    data_dir_path = f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}'
    if not os.path.isdir(data_dir_path):
        os.mkdir(data_dir_path)
    return data_dir_path


def create_data_file_path(dataset_name: str, version: str,
                          partitioned: bool) -> str:
    data_file_path = (
        f'{get_data_dir_path(dataset_name)}/'
        f'{dataset_name}__{semver.dotted_to_underscored(version)[:3]}'
    )
    return (data_file_path if partitioned else f'{data_file_path}.parquet')


def create_metadata_file_path(dataset_name: str, version: str) -> str:
    return (
        f'{get_metadata_dir_path(dataset_name)}/'
        f'{dataset_name}__{semver.dotted_to_underscored(version)}'
    )


def is_dataset_in_data_store(dataset_name: str, release_status) -> bool:
    data_store = get_datastore_info()
    for version in data_store["versions"]:
        for dataset in version["dataStructureUpdates"]:
            if(
                dataset["name"] == dataset_name and
                dataset["releaseStatus"] == release_status
            ):
                return True
    return False


class DatasetNotFound(Exception):
    pass


class NoSuchPendingOperation(Exception):
    pass
