import json
import shutil

import pytest

from datastore_version_manager.domain import metadata_patcher
from datastore_version_manager.domain.metadata_patcher import PatchMetadataException

PATCH_METADATA_DIR = 'tests/resources/patch_metadata'
WITH_CODE_LIST_DIR = f'{PATCH_METADATA_DIR}/dataset_with_code_list'
WITHOUT_CODE_LIST_DIR = f'{PATCH_METADATA_DIR}/dataset_without_code_list'
INVALID_DIR = f'{PATCH_METADATA_DIR}/invalid'


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


def test_patch_metadata_with_code_list():
    source = load_file(f'{WITH_CODE_LIST_DIR}/source_SYNT_BEFOLKNING_KJOENN.json')
    destination = load_file(f'{WITH_CODE_LIST_DIR}/destination_SYNT_BEFOLKNING_KJOENN.json')
    expected = load_file(f'{WITH_CODE_LIST_DIR}/expected_SYNT_BEFOLKNING_KJOENN.json')

    actual = metadata_patcher.validate_patch(source, destination)
    assert actual == expected


def test_patch_metadata_without_code_list():
    source = load_file(f'{WITHOUT_CODE_LIST_DIR}/source_SYNT_PERSON_INNTEKT.json')
    destination = load_file(f'{WITHOUT_CODE_LIST_DIR}/destination_SYNT_PERSON_INNTEKT.json')
    expected = load_file(f'{WITHOUT_CODE_LIST_DIR}/expected_SYNT_PERSON_INNTEKT.json')

    actual = metadata_patcher.validate_patch(source, destination)
    assert actual == expected


def test_patch_metadata_illegal_fields_changes():
    """
    The source contains randomly chosen fields that are not allowed to be changed.
    """
    source = load_file(f'{INVALID_DIR}/source_SYNT_BEFOLKNING_KJOENN.json')
    destination = load_file(f'{INVALID_DIR}/destination_SYNT_BEFOLKNING_KJOENN.json')

    with pytest.raises(PatchMetadataException) as e:
        metadata_patcher.validate_patch(source, destination)

    expected = str(e.value).replace('\'', '').replace('"', '')
    assert "root[shortName]: {new_value: SYNT_BEFOLKNING_KJOENN2, old_value: SYNT_BEFOLKNING_KJOENN" in expected
    assert "root[temporalityType]: {new_value: FIXED2, old_value: FIXED}" in expected
    assert "root[identifierVariables][0][shortName]: {new_value: PERSONID_2, old_value: PERSONID_1}" in expected
    assert "root[identifierVariables][0][unitType][shortName]: {new_value: PERSON2, old_value: PERSON}" in expected
    assert "root[identifierVariables][0][valueDomain][shortName]: {new_value: PERSONID_2, old_value: PERSONID_1}" in expected
    assert "root[measureVariables][0][subjectFields][0][shortName]: {new_value: HELSE2, old_value: HELSE}" in expected
    assert "root[measureVariables][0][subjectFields][1][shortName]: {new_value: HELSETJENESTER2, old_value: HELSETJENESTER}" in expected
    assert "root[measureVariables][0][valueDomain][codeList][codeItems][0][code]: {new_value: 12, old_value: 1}" in expected
    assert "root[measureVariables][0][valueDomain][codeList][codeItems][0][validFrom]: {new_value: 2000-01-01, old_value: 1900-01-01}" in expected


def load_file(file_name: str):
    with open(file_name) as f:
        source = json.load(f)
    return source
