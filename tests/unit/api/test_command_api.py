from flask import url_for
from datastore_version_manager.domain import pending_operations
from datastore_version_manager.service import draft_dataset_service

MOCKED_DATASTRUCTURE_UPDATES = [
    {
        "description": "mocked",
        "name": "MOCKED",
        "operation": "MOCK",
        "releaseStatus": "MOCK"
    }
]

ADD_REQUEST = {
    'operation_type': 'ADD',
    'dataset_name': 'MOCK_DATASET',
    'description': 'my mocked dataset'
}

UPDATE_REQUEST = {
    'release_status': 'PENDING_RELEASE'
}


def test_get_pending_operations(flask_app, mocker):
    mocker.patch.object(
        pending_operations, 'get_datastructure_updates',
        return_value=MOCKED_DATASTRUCTURE_UPDATES
    )
    response = flask_app.get(url_for('command_api.get_pending_operations'))
    assert response.json == MOCKED_DATASTRUCTURE_UPDATES


def test_post_pending_operations_add(flask_app, mocker):
    spy = mocker.patch.object(
        draft_dataset_service, 'add_new_draft_dataset', return_value=None
    )
    response = flask_app.post(
        url_for('command_api.add_pending_operation'),
        json=ADD_REQUEST
    )
    spy.assert_called_with(
        ADD_REQUEST['operation_type'],
        ADD_REQUEST['dataset_name'],
        ADD_REQUEST['description'],
        False
    )
    assert response.status_code == 200
    assert response.json == {'message': 'OK'}


def test_post_pending_operations_change_data():
    ...


def test_post_pending_operations_pending_delete():
    ...


def test_update_pending_operation(flask_app, mocker):
    spy = mocker.patch.object(
        draft_dataset_service, 'update_pending_operation', return_value=None
    )
    response = flask_app.put(
        url_for('command_api.update_pending_operation', dataset_name="MOCK_DATASET"),
        json=UPDATE_REQUEST
    )
    spy.assert_called_with(
        'MOCK_DATASET',
        UPDATE_REQUEST['release_status'],
        None
    )
    assert response.status_code == 200
    assert response.json == {'message': 'OK'}


def test_update_pending_operation_forbidden():
    ...
