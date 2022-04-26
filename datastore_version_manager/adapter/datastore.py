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
        json.dump(pending_operation, f, indent=4, ensure_ascii=False)


def get_datastore_versions() -> dict:
    datastore_versions_json = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/datastore_versions.json'
    )
    with open(datastore_versions_json, encoding="utf-8") as f:
        return json.load(f)


def write_datastore_versions(datastore_dict: dict):
    datastore_versions_json = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/datastore_versions.json'
    )
    with open(datastore_versions_json, 'w', encoding="utf-8") as f:
        return json.dump(datastore_dict, f, indent=4, ensure_ascii=False)


def write_to_archive(json_dict: dict, file_path: str):
    file_path = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/archive/{file_path}'
    )
    with open(file_path, 'w', encoding="utf-8") as f:
        json.dump(json_dict, f, indent=4, ensure_ascii=False)


def get_archive(file_path: str) -> list:
    file_path = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/archive/{file_path}'
    )
    return os.listdir(file_path)


def remove_archived_pending_operations(pre_bump_pending_operations: dict, new_version: str):
    file_path = (
        f'pending_operations/pending_operations__before_{semver.dotted_to_underscored(new_version)}.json'
    )
    write_to_archive(pre_bump_pending_operations, file_path)

    pending_ops_archive = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/archive/pending_operations'
    )
    shutil.rmtree(pending_ops_archive)
    os.mkdir(pending_ops_archive)


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
    os.remove(f'{metadata_dir}/{dataset_name}__DRAFT.json')
    if len(os.listdir(metadata_dir)) == 0:
        shutil.rmtree(metadata_dir)

    data_dir = get_data_dir_path(dataset_name)
    single_parquet = f'{data_dir}/{dataset_name}__DRAFT.parquet'
    if os.path.exists(single_parquet):
        os.remove(single_parquet)
    else:
        partitioned_parquet = f'{data_dir}/{dataset_name}__DRAFT'
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


def get_metadata_file_path(dataset_name: str, version: str) -> str:
    return (
        f'{get_metadata_dir_path(dataset_name)}/'
        f'{dataset_name}__{semver.dotted_to_underscored(version)}.json'
    )


def get_metadata(dataset_name: str, version: str) -> dict:
    metadata_json = (
        get_metadata_file_path(
            dataset_name, version
        )
    )
    with open(metadata_json, encoding="utf-8") as f:
        return json.load(f)


def change_draft_metadata_file_name(dataset_name: str, version: str) -> None:
    source_metadata_json = (
        get_metadata_file_path(
            dataset_name, "DRAFT"
        )
    )
    destination_metadata_json = (
        get_metadata_file_path(
            dataset_name, version
        )
    )

    if os.path.exists(destination_metadata_json):
        raise Exception(f"{destination_metadata_json} should not exist.")

    os.rename(source_metadata_json, destination_metadata_json)


def get_data_dir_path(dataset_name: str) -> str:
    return f'{os.environ["DATASTORE_ROOT_DIR"]}/data/{dataset_name}'


def create_data_dir_path(dataset_name: str) -> str:
    data_dir_path = get_data_dir_path(dataset_name)
    if not os.path.isdir(data_dir_path):
        os.mkdir(data_dir_path)
    return data_dir_path


def get_data_file_path(dataset_name: str, version: str) -> str:
    data_file_path = (
        f'{get_data_dir_path(dataset_name)}/'
        f'{dataset_name}__{semver.dotted_to_underscored(version, 2)}'
    )
    if os.path.isdir(data_file_path):
        return data_file_path
    else:
        return f'{data_file_path}.parquet'


def is_data_file_partitioned(dataset_name: str, version: str) -> bool:
    data_file_path = get_data_file_path(dataset_name, version)
    if os.path.isdir(data_file_path):
        return True
    else:
        return False


def change_draft_data_file_name(dataset_name: str, version: str) -> None:
    source_data_parquet = (
        get_data_file_path(
            dataset_name, "DRAFT"
        )
    )
    destination_data_parquet = (
        f'{get_data_dir_path(dataset_name)}/'
        f'{dataset_name}__{semver.dotted_to_underscored(version, 2)}'
    )

    if not is_data_file_partitioned(dataset_name, "DRAFT"):
        destination_data_parquet = (
            f'{destination_data_parquet}.parquet'
        )

    if os.path.exists(destination_data_parquet):
        raise Exception(f"{destination_data_parquet} should not exist.")

    os.rename(source_data_parquet, destination_data_parquet)


def get_metadata_all(version: str) -> str:
    """
    Get metadata_all__x_x_x_x.json
    :param version : str
        Either "DRAFT" or a semantic version ("x_x_x_x" where x is a number)
    """
    metadata_all_file_path = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/'
        f'metadata_all__{semver.dotted_to_underscored(version)}.json'
    )
    with open(metadata_all_file_path, 'r') as f:
        return json.load(f)


def write_metadata_all(metadata_all: dict, version: str) -> None:
    metadata_all_file_path = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/'
        f'metadata_all__{semver.dotted_to_underscored(version)}.json'
    )
    with open(metadata_all_file_path, 'w') as f:
        json.dump(metadata_all, f, indent=4, ensure_ascii=False)


def is_dataset_in_datastore_versions(dataset_name: str, release_status) -> bool:
    """
    Determines if dataset has existed in the datastore.
    :return: This function returns True also in case dataset was RELEASED and then DELETED.
    """
    datastore_versions = get_datastore_versions()
    for version in datastore_versions["versions"]:
        for dataset in version["dataStructureUpdates"]:
            if(
                dataset["name"] == dataset_name and
                dataset["releaseStatus"] == release_status
            ):
                return True
    return False


def get_latest_version():
    datastore_versions = get_datastore_versions()

    if len(datastore_versions["versions"]) == 0:
        return "DRAFT"

    return datastore_versions["versions"][0]["version"]


def get_data_versions(version: str) -> dict:
    """
    data_versions__x_x_x_x.json is generated for each version and points to the correct data file.
    :param version:
    """
    data_versions_file_path = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/'
        f'data_versions__{semver.dotted_to_underscored(version)}.json'
    )
    with open(data_versions_file_path, 'r') as f:
        return json.load(f)


def write_data_versions(data_versions: dict, version: str) -> None:
    data_versions_file_path = (
        f'{os.environ["DATASTORE_ROOT_DIR"]}/datastore/'
        f'data_versions__{semver.dotted_to_underscored(version)}.json'
    )
    with open(data_versions_file_path, 'w') as f:
        json.dump(data_versions, f, indent=4, ensure_ascii=False)


class DatasetNotFound(Exception):
    pass


class NoSuchPendingOperation(Exception):
    pass
