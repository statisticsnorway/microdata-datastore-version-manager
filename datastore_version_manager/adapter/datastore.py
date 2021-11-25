import json
import os


def get_pending_operations_json() -> dict:
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/pending_operations.json', encoding = "utf-8") as f:
            return json.load(f)


def write_pending_operations_json(pending_operation: dict):
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/pending_operations.json', 'w', encoding = "utf-8") as f:
        json.dump(pending_operation, f, indent=4)


def get_datastore_json() -> dict:
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/data_store.json', encoding = "utf-8") as f:
            return json.load(f)


def write_datastore_json(datastore_dict: dict):
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/data_store.json', 'w', encoding = "utf-8") as f:
            return json.dump(datastore_dict, f, indent=4)


def dataset_exists(dataset_name):
    return(
        os.path.isdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}') and
        os.path.isdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}')
    )

def new_dataset_directory(dataset_name: str) -> str:
    os.mkdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}')
    os.mkdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}')


def get_metadata_dir_path(dataset_name: str) -> str:
    return f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}'


def get_data_dir_path(dataset_name: str) -> str:
    return f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}'
