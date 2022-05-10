import json
import shutil

from datastore_version_manager.domain import metadata_patcher

PATCH_METADATA_DIR = 'tests/resources/patch_metadata'


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


def test_patch_metadata():
    # source_json has changes in fields that will be skipped (temporality)
    source_json = f'{PATCH_METADATA_DIR}/source_TEST_DATASET__DRAFT.json'
    with open(source_json) as f:
        source = json.load(f)

    destination_json = f'{PATCH_METADATA_DIR}/destination_TEST_DATASET__DRAFT.json'
    with open(destination_json) as f:
        destination = json.load(f)

    expected_json = f'{PATCH_METADATA_DIR}/expected_TEST_DATASET__DRAFT.json'
    with open(expected_json) as f:
        expected = json.load(f)

    actual = metadata_patcher.patch(source, destination)
    assert actual == expected
