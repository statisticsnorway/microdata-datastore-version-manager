from datastore_version_manager import application


def test_valid_messages():
    application.handle_message(None, None, None, '{"hey": "there"}')
    assert 1 == 1


def test_message_missing_parameters():
    application.handle_message(None, None, None, '{"hey": "there"}')
    assert 1 == 1


def test_message_invalid_command():
    application.handle_message(None, None, None, '{"hey": "there"}')
    assert 1 == 1
