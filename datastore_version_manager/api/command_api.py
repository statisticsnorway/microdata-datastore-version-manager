import logging

from flask import Blueprint, jsonify
from flask_pydantic import validate

from datastore_version_manager.domain import (
    pending_operations,
    draft_dataset,
    version_bumper,
    datastore_versions
)
from datastore_version_manager.exceptions.exceptions import (
    ForbiddenOperation
)
from datastore_version_manager.api.request_models import (
    NewPendingOperationRequest,
    UpdatePendingOperationRequest,
    ApplyBumpManifestoRequest
)

logger = logging.getLogger()
command_api = Blueprint('command_api', __name__)


@command_api.route('/pending-operations', methods=['GET'])
@validate()
def get_pending_operations():
    logger.info('GET /pending-operations')
    return jsonify(pending_operations.get_datastructure_updates())


@command_api.route('/pending-operations', methods=['POST'])
@validate()
def add_pending_operation(body: NewPendingOperationRequest):
    logger.info(f'POST /pending-operations with body {body}')
    operation_type = body.operationType
    if operation_type == 'ADD_OR_CHANGE_DATA':
        dataset_name = body.datasetName
        description = body.description
        draft_dataset.add_new_draft_dataset(operation_type, dataset_name, description)
        return {"message": "OK"}
    elif operation_type == 'PATCH_METADATA':
        dataset_name = body.datasetName
        description = body.description
        draft_dataset.add_new_draft_dataset(operation_type, dataset_name, description)
        return {"message": "OK"}
    elif operation_type == 'REMOVE':
        dataset_name = body.datasetName
        description = body.description
        pending_operations.add_new(dataset_name, 'REMOVE', 'PENDING_DELETE', description)
        return {"message": "OK"}
    else:
        raise ForbiddenOperation(f"Forbidden operation: {operation_type}")


@command_api.route('/pending-operations/<dataset_name>', methods=['DELETE'])
@validate()
def delete_pending_operation(dataset_name):
    logger.info(f'DELETE /pending-operations/{dataset_name}')
    pending_operations.remove(dataset_name)
    return {"message": "OK"}


@command_api.route('/pending-operations/<dataset_name>', methods=['PUT'])
@validate()
def update_pending_operation(dataset_name, body: UpdatePendingOperationRequest):
    logger.info(f'PUT /pending-operations/{dataset_name} with body {body}')
    release_status = body.releaseStatus
    description = body.description
    draft_dataset.update_pending_operation(dataset_name, release_status, description)
    return {"message": "OK"}


@command_api.route('/datastore/bump', methods=['GET'])
@validate()
def get_bump_manifesto():
    return jsonify(version_bumper.get_bump_manifesto())


@command_api.route('/datastore/bump', methods=['POST'])
@validate()
def apply_bump_manifesto(body: ApplyBumpManifestoRequest):
    desc = body.description
    client_bump_manifesto = [op.dict() for op in body.pendingOperations]
    version_bumper.apply_bump_manifesto(client_bump_manifesto, desc)
    return {"message": "OK"}


@command_api.route('/released-datasets', methods=['GET'])
@validate()
def get_released_datasets():
    logger.info(f'GET /released-datasets')
    return datastore_versions.get_released_datasets()
