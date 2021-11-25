import os


def get_dataset_path(dataset_name: str) -> str:
    dataset_dir = os.environ['DATASET_BUILDER_OUTPUT_DIR']
    for dataset_path in os.listdir(dataset_dir):
        if dataset_path == dataset_name:
            return f'{dataset_dir}/{dataset_name}'
