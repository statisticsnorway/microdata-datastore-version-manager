import json
import os
import shutil


def get_pending_operations() -> dict:
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/pending_operations.json', encoding="utf-8") as f:
        return json.load(f)


def write_pending_operations(pending_operation: dict):
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/pending_operations.json', 'w', encoding="utf-8") as f:
        json.dump(pending_operation, f, indent=4)


<<<<<<< HEAD
=======
def remove_dataset_from_pending_operations(dataset_name: str):
    pending_operations_dict = get_pending_operations()
    pending_operations = pending_operations_dict["dataStructureUpdates"]
    if not any(dataset['name'] == dataset_name for dataset in pending_operations):
        raise DatasetNotFound(
            f'Dataset {dataset_name} not found in pending_operations.json'
        )
    for i in range(len(pending_operations)):
        if pending_operations[i]['name'] == dataset_name:
            del pending_operations[i]
            break
    write_pending_operations(pending_operations_dict)


>>>>>>> ae467d1d497ce4a3b0bb5c908c0309856957ed59
def get_datastore_info() -> dict:
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/data_store.json', encoding="utf-8") as f:
        return json.load(f)


def write_datastore_info(datastore_dict: dict):
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/data_store.json', 'w', encoding="utf-8") as f:
        return json.dump(datastore_dict, f, indent=4)


def dataset_exists(dataset_name: str):
    return (
            os.path.isdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}') and
            os.path.isdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}')
    )


def draft_dataset_exists(dataset_name: str):
    pending_operations = get_pending_operations()
    datasets = pending_operations["dataStructureUpdates"]
    dataset_list = [dataset for dataset in datasets if dataset['name'] == dataset_name]
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
    return f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}'


def get_data_dir_path(dataset_name: str) -> str:
    return f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}'


def is_dataset_in_data_store(dataset_name: str, release_status) -> bool:
    data_store = get_datastore_info()
    for version in data_store["versions"]:
        for dataset in version["dataStructureUpdates"]:
            if dataset["name"] == dataset_name and dataset["releaseStatus"] == release_status:
                return True
    return False


class DatasetNotFound(Exception):
    pass


class NoSuchPendingOperation(Exception):
    pass
