import glob
import json
import os
from distutils.version import StrictVersion


def get_pending_operations() -> dict:
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/pending_operations.json', encoding="utf-8") as f:
        return json.load(f)


def write_pending_operations(pending_operation: dict):
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/pending_operations.json', 'w', encoding="utf-8") as f:
        json.dump(pending_operation, f, indent=4)


def get_datastore_info() -> dict:
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/data_store.json', encoding="utf-8") as f:
        return json.load(f)


def write_datastore_info(datastore_dict: dict):
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/data_store.json', 'w', encoding="utf-8") as f:
        return json.dump(datastore_dict, f, indent=4)


def dataset_exists(dataset_name):
    return (
            os.path.isdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}') and
            os.path.isdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}')
    )


def new_dataset_directory(dataset_name: str) -> str:
    os.mkdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}')
    os.mkdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}')


def get_metadata_dir_path(dataset_name: str) -> str:
    return f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}'


def get_data_dir_path(dataset_name: str) -> str:
    return f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}'


def find_latest_in_metadata_all(dataset_name: str) -> str:
    def convert_file_name(file_name: str) -> str:
        return os.path.basename(file_name) \
            .replace(f'metadata_all__', '') \
            .replace('.json', '') \
            .replace('_', '.')

    file_names_with_paths = glob.glob(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/metadata_all__*.json')
    versions = [convert_file_name(file_name) for file_name in file_names_with_paths]
    versions.sort(key=StrictVersion)

    newest_version = versions[-1].replace('.', '_')
    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/metadata_all__{newest_version}.json', encoding="utf-8") as f:
        metadata_all = json.load(f)
        try:
            dataset = next(
                data_structure for data_structure in metadata_all['dataStructures']
                if data_structure["name"] == dataset_name
            )
            return dataset
        except StopIteration:
            raise DatasetNotFound(
                f'Dataset {dataset_name} not found in newest metadata_all'
                f' metadata_all__{newest_version}.json'
            )


class DatasetNotFound(Exception):
    pass


class NoSuchPendingOperation(Exception):
    pass
