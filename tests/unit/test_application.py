import json
import shutil
from datastore_version_manager import application
from datastore_version_manager import commands


BUMP_VERSION_MESSAGE = json.dumps({
    "jobId": "123",
    "command": "BUMP_VERSION",
    "parameters": {"description": "test"}
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


def test_message_missing_parameters():
    application.handle_message(None, None, None, '{"hey": "there"}')
    assert 1 == 1


def test_message_invalid_command():
    application.handle_message(None, None, None, '{"hey": "there"}')
    assert 1 == 1
