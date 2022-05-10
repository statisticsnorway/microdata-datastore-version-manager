from flask import url_for
from datastore_version_manager.domain import pending_operations, draft_dataset, version_bumper

MOCKED_DATASTRUCTURE_UPDATES = [
    {
        "description": "mocked",
        "name": "MOCKED",
        "operation": "MOCK",
        "releaseStatus": "MOCK"
    }
]


MOCKED_BUMP_MANIFESTO = [
    {
        "description": "mocked",
        "name": "MOCKED A",
        "operation": "MOCK",
        "releaseStatus": "PENDING_RELEASE"
    }
]


ADD_REQUEST = {
    'operationType': 'ADD_OR_CHANGE_DATA',
    'datasetName': 'MOCK_DATASET',
    'description': 'my mocked dataset'
}


PATCH_METADATA_REQUEST = {
    'operationType': 'PATCH_METADATA',
    'datasetName': 'MOCK_DATASET',
    'description': 'my mocked dataset'
}


REMOVE_REQUEST = {
    'operationType': 'REMOVE',
    'datasetName': 'MOCK_DATASET',
    'description': 'my mocked dataset'
}

UPDATE_REQUEST = {
    'releaseStatus': 'PENDING_RELEASE'
}


APPLY_BUMP_REQUEST = {
    'pendingOperations': MOCKED_BUMP_MANIFESTO,
    'description': 'description'
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
        draft_dataset, 'add_new_draft_dataset', return_value=None
    )
    response = flask_app.post(
        url_for('command_api.add_pending_operation'),
        json=ADD_REQUEST
    )
    spy.assert_called_with(
        ADD_REQUEST['operationType'],
        ADD_REQUEST['datasetName'],
        ADD_REQUEST['description']
    )
    assert response.status_code == 200
    assert response.json == {'message': 'OK'}


def test_post_pending_operations_patch_metadata(flask_app, mocker):
    spy = mocker.patch.object(
        draft_dataset, 'add_new_draft_dataset', return_value=None
    )
    response = flask_app.post(
        url_for('command_api.add_pending_operation'),
        json=PATCH_METADATA_REQUEST
    )
    spy.assert_called_with(
        PATCH_METADATA_REQUEST['operationType'],
        PATCH_METADATA_REQUEST['datasetName'],
        PATCH_METADATA_REQUEST['description']
    )
    assert response.status_code == 200
    assert response.json == {'message': 'OK'}


def test_post_pending_operations_remove(flask_app, mocker):
    spy = mocker.patch.object(
        pending_operations, 'add_new', return_value=None
    )
    response = flask_app.post(
        url_for('command_api.add_pending_operation'),
        json=REMOVE_REQUEST
    )
    spy.assert_called_with(
        REMOVE_REQUEST['datasetName'],
        REMOVE_REQUEST['operationType'],
        'PENDING_DELETE',
        REMOVE_REQUEST['description']
    )
    assert response.status_code == 200
    assert response.json == {'message': 'OK'}


def test_delete_pending_operations(flask_app, mocker):
    mocker.patch.object(
        pending_operations, 'remove', return_value=None
    )
    response = flask_app.delete(
        url_for('command_api.delete_pending_operation', dataset_name='MOCK_DATASET')
    )
    assert response.status_code == 200
    assert response.json == {'message': 'OK'}


def test_update_pending_operation(flask_app, mocker):
    spy = mocker.patch.object(
        draft_dataset, 'update_pending_operation', return_value=None
    )
    response = flask_app.put(
        url_for('command_api.update_pending_operation', dataset_name="MOCK_DATASET"),
        json=UPDATE_REQUEST
    )
    spy.assert_called_with(
        'MOCK_DATASET',
        UPDATE_REQUEST['releaseStatus'],
        None
    )
    assert response.status_code == 200
    assert response.json == {'message': 'OK'}


def test_get_bump_manifesto(flask_app, mocker):
    mocker.patch.object(
        version_bumper, 'get_bump_manifesto',
        return_value=MOCKED_BUMP_MANIFESTO
    )
    response = flask_app.get(url_for('command_api.get_bump_manifesto'))
    assert response.json == MOCKED_BUMP_MANIFESTO


def test_apply_bump_manifesto(flask_app, mocker):
    spy = mocker.patch.object(
        version_bumper, 'apply_bump_manifesto',
        return_value=None
    )
    flask_app.post(
        url_for('command_api.apply_bump_manifesto'),
        json=APPLY_BUMP_REQUEST
    )
    spy.assert_called_with(
        APPLY_BUMP_REQUEST['pendingOperations'],
        APPLY_BUMP_REQUEST['description']
    )
