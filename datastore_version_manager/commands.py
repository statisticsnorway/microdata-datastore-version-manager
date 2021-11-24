from datastore_version_manager import datastore
from datastore_version_manager import util


def new_draft_to_pending_operations(dataset_name: str, description: str,
                                    operation: str):
    pending_operations = datastore.get_pending_operations_json()
    pending_operations_list = pending_operations["pendingOperations"]
    pending_operations_list.append({
        "dataSetName" : dataset_name,
        "operation" : operation,
        "description" : description,
        "releaseStatus": "DRAFT"
    })
    pending_operations["version"] = util.bump_draftpatch(
        pending_operations["version"]
    )

def set_status_to_pending_release(dataset_name: str):
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
    except StopIteration:
        raise noSuchDraftException(
            f'Unable to change release status of {dataset_name}.'
            ' {dataset_name} not in list of drafts'
        )
