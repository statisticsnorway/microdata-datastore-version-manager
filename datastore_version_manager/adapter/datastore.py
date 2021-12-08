import json
import os


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


def draft_dataset_exists(dataset_name: str):
    data_store = get_datastore_info()
    datasets = data_store["versions"][0]["dataStructureUpdates"]
    dataset_list = [dataset for dataset in datasets if dataset['name'] == dataset_name]
    return True if len(dataset_list)>0 else False


def new_dataset_directory(dataset_name: str) -> None:
    os.mkdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}')
    os.mkdir(f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}')


def get_metadata_dir_path(dataset_name: str) -> str:
    return f'{os.environ["DATASTORE_ROOT_DIR"]}/metadata/{dataset_name}'


def get_data_dir_path(dataset_name: str) -> str:
    return f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}'


def find_latest_in_metadata_all(dataset_name: str) -> str:
    # using the fact that versions in data_store.json are in descending order, first being draft data store version
    datastore_info = get_datastore_info()
    latest_version = datastore_info["versions"][1]["version"]
    latest_version = "_".join(latest_version.replace('.', '_').split("_")[:-1])

    with open(f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/metadata_all__{latest_version}.json',
              encoding="utf-8") as f:
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
                f' metadata_all__{latest_version}.json'
            )


class DatasetNotFound(Exception):
    pass


class NoSuchPendingOperation(Exception):
    pass
