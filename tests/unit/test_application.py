import json
import shutil
from datastore_version_manager import application
from datastore_version_manager import commands


BUMP_VERSION_MESSAGE = json.dumps({
    "jobId": "123",
    "command": "BUMP_VERSION",
    "parameters": {"description": "test"}
})

ADD_DATASET_MESSAGE = json.dumps({
    "jobId": "123",
    "command": "ADD_DATASET",
    "parameters": {"datasetName": "MY_DATASET"}
})

SET_STATUS_MESSAGE = json.dumps({
    "jobId": "123",
    "command": "SET_STATUS",
    "parameters": {
        "datasetName": "MY_DATASET",
        "description": "test-description",
        "operation": "test-operation",
        "releaseStatus": "test-releaseStatus"
    }
})

HARD_DELETE_MESSAGE = json.dumps({
    "jobId": "123",
    "command": "HARD_DELETE",
    "parameters": {"datasetName": "MY_DATASET"}
})

MISSING_PARAMETERS_MESSAGE = json.dumps({
    "jobId": "123",
    "command": "SET_STATUS",
    "parameters": {
        "datasetName": "MY_DATASET"
    }
})

INVALID_COMMAND_MESSAGE = json.dumps({
    "jobId": "123",
    "command": "NOT_A_COMMAND",
    "parameters": {"datasetName": "MY_DATASET"}
})


def setup_function():
    shutil.copytree(
        'tests/resources',
        'tests/resources_backup'
    )


def teardown_function():
    shutil.rmtree('tests/resources')
    shutil.move(
        'tests/resources_backup',
        'tests/resources'
    )


def test_valid_messages(mocker):
    mocker.patch('datastore_version_manager.commands.bump_version')
    spy = mocker.spy(commands, "bump_version")
    application.handle_message(None, None, None, BUMP_VERSION_MESSAGE)
    spy.assert_called_once_with("test")

    mocker.patch('datastore_version_manager.commands.add_new_dataset')
    spy = mocker.spy(commands, "add_new_dataset")
    application.handle_message(None, None, None, ADD_DATASET_MESSAGE)
    spy.assert_called_once_with("MY_DATASET")

    mocker.patch('datastore_version_manager.commands.hard_delete')
    spy = mocker.spy(commands, "hard_delete")
    application.handle_message(None, None, None, HARD_DELETE_MESSAGE)
    spy.assert_called_once_with("MY_DATASET")

    mocker.patch('datastore_version_manager.commands.set_status')
    spy = mocker.spy(commands, "set_status")
    application.handle_message(None, None, None, SET_STATUS_MESSAGE)
    spy.assert_called_once_with(
        "MY_DATASET",
        "test-releaseStatus",
        "test-operation",
        "test-description"
    )


def test_message_missing_parameters(mocker):
    logger_spy = mocker.spy(application.logger, "error")
    spy = mocker.spy(commands, "set_status")
    application.handle_message(None, None, None, MISSING_PARAMETERS_MESSAGE)
    spy.assert_not_called()
    logger_spy.assert_called_with("'operation' is a required property")


def test_message_invalid_command(mocker):
    logger_spy = mocker.spy(application.logger, "error")
    application.handle_message(None, None, None, INVALID_COMMAND_MESSAGE)
    logger_spy.assert_called_with(
        "'NOT_A_COMMAND' is not one of "
        "['ADD_DATASET', 'BUMP_VERSION', 'HARD_DELETE', 'SET_STATUS']"
    )
