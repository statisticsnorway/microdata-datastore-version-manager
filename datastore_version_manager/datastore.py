import json
import os


DATASTORE_ROOT_DIR = os.environ['DATASTORE_ROOT_DIR']


def get_pending_operations_json() -> dict:
    with open(f'{DATASTORE_ROOT_DIR}/datastore/pending_operations.json') as f:
            return json.load(f)


def write_pending_operations_json(pending_operation: dict):
    with open(f'{DATASTORE_ROOT_DIR}/datastore/pending_operations.json', 'w') as f:
        json.dump(pending_operation, f)


def get_datastore_json() -> dict:
    with open(f'{DATASTORE_ROOT_DIR}/datastore/data-store.json') as f:
            return json.load(f)


def write_datastore_json(datastore_dict: dict):
    with open(f'{DATASTORE_ROOT_DIR}/datastore/data-store.json', 'w') as f:
            return json.dump(f)


def dataset_exists(dataset_name):
    return(
        os.isdir(f'{DATASTORE_ROOT_DIR}/data/{dataset_name}') and
        os.isdir(f'{DATASTORE_ROOT_DIR}/data/{dataset_name}')
    )

def new_dataset_directory(dataset_name: str):
    os.mkdir(f'{DATASTORE_ROOT_DIR}/data/{dataset_name}')
    os.mkdir(f'{DATASTORE_ROOT_DIR}/metadata/{dataset_name}')
