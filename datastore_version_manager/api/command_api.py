import logging

from flask import Blueprint, jsonify
from flask_pydantic import validate

from datastore_version_manager.service import draft_dataset_service
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
    operation_type = body.operation_type
    if operation_type == 'ADD':
        dataset_name = body.dataset_name
        description = body.description
        draft_dataset_service.add_new_draft_dataset(operation_type, dataset_name, description, False)
        return {"message": "OK"}
    elif operation_type == 'CHANGE_DATA':
        dataset_name = body.dataset_name
        description = body.description
        draft_dataset_service.add_new_draft_dataset(operation_type, dataset_name, description, True)
        return {"message": "OK"}
    elif operation_type == 'PENDING_DELETE':
        # TODO: implement this case in draft_dataset_service
        return {"message": "Not implemented"}, 500
    else:
        raise ForbiddenOperation(f"Forbidden operation: {operation_type}")


@command_api.route('/pending-operations/<dataset_name>', methods=['DELETE'])
@validate()
def delete_pending_operation(body: RemovePendingOperationRequest):
    # TODO: implement this case in draft_dataset_service
    return {"message": "Not implemented"}, 500


@command_api.route('/pending-operations/<dataset_name>', methods=['PUT'])
@validate()
def update_pending_operation(dataset_name, body: UpdatePendingOperationRequest):
    logger.info(f'PUT /pending-operations/{dataset_name} with body {body}')
    release_status = body.release_status
    operation_type = body.operation_type
    draft_dataset_service.update_pending_operation(dataset_name, release_status, operation_type)
    return {"message": "OK"}


@command_api.route('/datastore/bump', methods=['GET'])
@validate()
def get_bump_manifesto():
    # TODO: implement this case in version_bumper
    return {"message": "Not implemented"}, 500


@command_api.route('/datastore/bump', methods=['POST'])
@validate()
def apply_bump_manifesto(body: ApplyBumpManifestoRequest):
    # TODO: implement this case in version_bumper
    return {"message": "Not implemented"}, 500
