import os


def get_metadata_path(dataset_name: str) -> str:
    built_dataset_dir = os.environ['BUILT_DATASETS_DIR']
    metadata_path = (
        f'{built_dataset_dir}/{dataset_name}/{dataset_name}__DRAFT.json'
    )
    if os.path.exists(metadata_path):
        return metadata_path
    else:
        raise NoBuiltDataset(
            f"No built metadata file for {dataset_name}"
        )


def get_data_path(dataset_name: str) -> str:
    built_dataset_dir = os.environ['BUILT_DATASETS_DIR']
    partitioned_parquet_path = (
        f'{built_dataset_dir}/{dataset_name}/{dataset_name}__DRAFT'
    )
    parquet_path = f'{partitioned_parquet_path}.parquet'

    if os.path.exists(partitioned_parquet_path):
        return partitioned_parquet_path
    elif os.path.exists(parquet_path):
        return parquet_path
    else:
        raise NoBuiltDataset(
            f"No built data file for {dataset_name}"
        )


class NoBuiltDataset(Exception):
    pass
