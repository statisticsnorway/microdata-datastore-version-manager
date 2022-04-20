import logging

from flask import Blueprint, jsonify
from flask_pydantic import validate

from datastore_version_manager.service import versioning_service
from datastore_version_manager.domain import pending_operations
from datastore_version_manager.exceptions.exceptions import (
    ForbiddenOperation
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
    return jsonify(pending_operations.get_datastructure_updates())


@command_api.route('/pending-operations', methods=['POST'])
@validate()
def add_pending_operation(body: NewPendingOperationRequest):
    logger.info(f'POST /pending-operations with body {body}')
    operation_type = body.operationType
    if operation_type == 'ADD_DATA':
        dataset_name = body.datasetName
        description = body.description
        versioning_service.add_new_draft_dataset(dataset_name, description, False)
        return {"message": "OK"}
    elif operation_type == 'CHANGE_DATA':
        # TODO: implement this case in versioning_service
        return {"message": "Not implemented"}, 500
    elif operation_type == 'PENDING_DELETE':
        # TODO: implement this case in versioning_service
        return {"message": "Not implemented"}, 500
    else:
        raise ForbiddenOperation(f"Forbidden operation: {operation_type}")


@command_api.route('/pending-operations/<dataset_name>', methods=['DELETE'])
@validate()
def delete_pending_operation(body: RemovePendingOperationRequest):
    # TODO: implement this case in versioning_service
    return {"message": "Not implemented"}, 500


@command_api.route('/pending-operations/<dataset_name>', methods=['PUT'])
@validate()
def set_status(body: UpdatePendingOperationRequest):
    # TODO: implement this case in versioning_service
    return {"message": "Not implemented"}, 500


@command_api.route('/datastore/bump', methods=['GET'])
@validate()
def get_bump_manifesto():
    # TODO: implement this case in versioning_service
    return {"message": "Not implemented"}, 500


@command_api.route('/datastore/bump', methods=['POST'])
@validate()
def apply_bump_manifesto(body: ApplyBumpManifestoRequest):
    # TODO: implement this case in versioning_service
    return {"message": "Not implemented"}, 500
