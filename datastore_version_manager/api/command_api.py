import logging

from flask import Blueprint, jsonify
from flask_pydantic import validate

from datastore_version_manager.service import versioning_service
from datastore_version_manager.domain.pending_operations import (
    get_datastructure_updates
)
from datastore_version_manager.api.request_models import (
    NewPendingOperationRequest,
    RemovePendingOperationRequest,
    UpdatePendingOperationRequest,
    ApplyBumpManifestoRequest
)

logger = logging.getLogger()
command_api = Blueprint('command_api', __name__)


@command_api.route('/pending-operations', methods=['GET'])
@validate()
def get_pending_operations():
    logger.info('GET /pending-operations')
    response = get_datastructure_updates()
    return jsonify(response)


@command_api.route('/pending-operations', methods=['POST'])
@validate()
def add_pending_operation(body: NewPendingOperationRequest):
    logger.info(f'POST /pending-operations with body {body}')
    operation_type = body.operation_type
    if operation_type == 'ADD_DATA':
        dataset_name = body.datasetName
        description = body.description
        versioning_service.add_new_dataset(dataset_name, description, False)
        return {"message": "OK"}
    if operation_type == 'CHANGE_DATA':
        # TODO: implement this case in versioning_service
        return {"message": "Not implemented"}
    elif operation_type == 'PENDING_DELETE':
        # TODO: implement this case in versioning_service
        return {"message": "Not implemented"}


@command_api.route('/pending-operations/<dataset_name>', methods=['DELETE'])
@validate()
def delete_pending_operation(body: RemovePendingOperationRequest):
    # TODO: implement this case in versioning_service
    return {"message": "Not implemented"}


@command_api.route('/pending-operations/<dataset_name>', methods=['PUT'])
@validate()
def set_status(body: UpdatePendingOperationRequest):
    # TODO: implement this case in versioning_service
    return {"message": "Not implemented"}


@command_api.route('/datastore/bump', methods=['GET'])
@validate()
def get_bump_manifesto():
    # TODO: implement this case in versioning_service
    return {"message": "Not implemented"}


@command_api.route('/datastore/bump', methods=['POST'])
@validate()
def apply_bump_manifesto(body: ApplyBumpManifestoRequest):
    # TODO: implement this case in versioning_service
    return {"message": "Not implemented"}
