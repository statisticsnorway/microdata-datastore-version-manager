from deepdiff import DeepDiff


def validate_patch(source_metadata: dict, destination_metadata: dict) -> dict:
    """
    Validates patching destination_metadata object with source_metadata object by checking the following:
        - there are no new fields
        - there are no removed fields
        - changes exist only in allowed fields
    The source_metadata effectively being the patched version of destination_metadata.
    :param source_metadata: Metadata of a dataset provided by user with patched fields
    :param destination_metadata: The latest versioned metadata of a dataset from datastore
    :return: validated source_metadata
    """

    ddiff = DeepDiff(destination_metadata, source_metadata,
                     exclude_regex_paths=[
                         r"root\['populationDescription'\]",
                         r"root\['identifierVariables'\]\[\d+\]\['name'\]",
                         r"root\['identifierVariables'\]\[\d+\]\['description'\]",
                         r"root\['identifierVariables'\]\[\d+\]\['unitType'\]\['name'\]",
                         r"root\['identifierVariables'\]\[\d+\]\['unitType'\]\['description'\]",
                         r"root\['identifierVariables'\]\[\d+\]\['valueDomain'\]\['description'\]",
                         r"root\['measureVariables'\]\[\d+\]\['name'\]",
                         r"root\['measureVariables'\]\[\d+\]\['description'\]",
                         r"root\['measureVariables'\]\[\d+\]\['subjectFields'\]\[\d+\]\['name'\]",
                         r"root\['measureVariables'\]\[\d+\]\['subjectFields'\]\[\d+\]\['description'\]",
                         r"root\['measureVariables'\]\[\d+\]\['valueDomain'\]\['codeList'\]\['codeItems'\]\[\d+\]"
                         r"\['categoryTitle'\]",
                         r"root\['measureVariables'\]\[\d+\]\['valueDomain'\]\['uriDefinition'\]",
                         r"root\['measureVariables'\]\[\d+\]\['valueDomain'\]\['sentinelAndMissingValues'\]\[\d+\]"
                         r"\['categoryTitle'\]",
                         r"root\['measureVariables'\]\[\d+\]\['valueDomain'\]\['description'\]",
                         r"root\['measureVariables'\]\[\d+\]\['valueDomain'\]\['measurementUnitDescription'\]",
                         r"root\['attributeVariables'\]\[\d+\]\['name'\]",
                         r"root\['attributeVariables'\]\[\d+\]\['description'\]",
                         r"root\['attributeVariables'\]\[\d+\]\['valueDomain'\]\['description'\]"
                     ])

    if 'dictionary_item_added' in ddiff:
        raise PatchMetadataException(f"Is is not allowed to add new metadata fields: {ddiff['dictionary_item_added']}")
    if 'dictionary_item_removed' in ddiff:
        raise PatchMetadataException(f"Is is not allowed to remove metadata fields: {ddiff['dictionary_item_removed']}")
    if 'values_changed' in ddiff:
        raise PatchMetadataException(f"There are changes in metadata fields that are not allowed: {ddiff['values_changed']}")

    return source_metadata


class PatchMetadataException(Exception):
    pass
