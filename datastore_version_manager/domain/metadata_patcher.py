from deepdiff import DeepDiff


def patch_metadata(source_metadata: dict, destination_metadata: dict) -> dict:
    """
    First it validates patching destination_metadata object with source_metadata object by checking the following:
        - there are no new fields
        - there are no removed fields
        - changes exist only in allowed fields
    The patched version consists of:
        - the source_metadata
        - selected validityPeriod fields from destination_metadata (which are null in source_metadata)
    :param source_metadata: Metadata of a dataset provided by user with patched fields
    :param destination_metadata: The latest versioned metadata of a dataset from datastore
    :return: validated and patched source_metadata
    """

    ddiff = DeepDiff(destination_metadata, source_metadata,
                     exclude_regex_paths=[
                         r"root\['populationDescription'\]",
                         r"root\['identifierVariables'\]\[\d+\]\['label'\]",
                         r"root\['identifierVariables'\]\[\d+\]\['representedVariables'\]\[\d+\]\['description'\]",
                         r"root\['identifierVariables'\]\[\d+\]\['representedVariables'\]\[\d+\]\['validPeriod'\]",
                         r"root\['identifierVariables'\]\[\d+\]\['keyType'\]\['label'\]",
                         r"root\['identifierVariables'\]\[\d+\]\['keyType'\]\['description'\]",
                         r"root\['identifierVariables'\]\[\d+\]\['representedVariables'\]\[\d+\]"
                         r"\['valueDomain'\]\['description'\]",
                         r"root\['measureVariable'\]\['label'\]",
                         r"root\['measureVariable'\]\['representedVariables'\]\[\d+\]\['description'\]",
                         r"root\['measureVariable'\]\['representedVariables'\]\[\d+\]\['validPeriod'\]",
                         r"root\['subjectFields'\]",
                         r"root\['measureVariable'\]\['representedVariables'\]\[\d+\]\['valueDomain'\]"
                         r"\['codeList'\]\[\d+\]\['category'\]",
                         r"root\['measureVariable'\]\['representedVariables'\]\[\d+\]\['valueDomain'\]\['description'\]",
                         r"root\['measureVariable'\]\['representedVariables'\]\[\d+\]\['valueDomain'\]\['unitOfMeasure'\]",
                         r"root\['attributeVariables'\]\[\d+\]\['label'\]",
                         r"root\['attributeVariables'\]\[\d+\]\['representedVariables'\]\[\d+\]\['description'\]",
                         r"root\['attributeVariables'\]\[\d+\]\['representedVariables'\]\[\d+\]\['validPeriod'\]",
                         r"root\['attributeVariables'\]\[\d+\]\['representedVariables'\]\[\d+\]\['valueDomain'\]"
                         r"\['description'\]"
                     ])

    if 'dictionary_item_added' in ddiff:
        raise PatchMetadataException(f"Is is not allowed to add new metadata fields: {ddiff['dictionary_item_added']}")
    if 'dictionary_item_removed' in ddiff:
        raise PatchMetadataException(f"Is is not allowed to remove metadata fields: {ddiff['dictionary_item_removed']}")
    if 'values_changed' in ddiff:
        raise PatchMetadataException(
            f"There are changes in metadata fields that are not allowed: {ddiff['values_changed']}")

    _patch_valid_periods(source_metadata, destination_metadata)
    _patch_temporal_coverage(source_metadata, destination_metadata)

    return source_metadata


def _patch_valid_periods(source_metadata: dict, destination_metadata: dict):
    for idx, identifier in enumerate(destination_metadata['identifierVariables']):
        for idx_repr_var, represented_var in enumerate(identifier['representedVariables']):
            source_metadata['identifierVariables'][idx]['representedVariables'][idx_repr_var]['validPeriod']['start'] \
                = represented_var['validPeriod']['start']
            if 'stop' in represented_var['validPeriod']:
                source_metadata['identifierVariables'][idx]['representedVariables'][idx_repr_var]['validPeriod']['stop'] \
                    = represented_var['validPeriod']['stop']
    for idx_repr_var, represented_var in enumerate(destination_metadata['measureVariable']['representedVariables']):
        if 'validPeriod' in source_metadata['measureVariable']['representedVariables'][idx_repr_var]:
            source_metadata['measureVariable']['representedVariables'][idx_repr_var]['validPeriod']['start'] \
                = represented_var['validPeriod']['start']
            if 'stop' in represented_var['validPeriod']:
                source_metadata['measureVariable']['representedVariables'][idx_repr_var]['validPeriod']['stop'] \
                    = represented_var['validPeriod']['stop']
    for idx, attribute in enumerate(destination_metadata['attributeVariables']):
        for idx_repr_var, represented_var in enumerate(attribute['representedVariables']):
            source_metadata['attributeVariables'][idx]['representedVariables'][idx_repr_var]['validPeriod']['start'] \
                = represented_var['validPeriod']['start']
            if 'stop' in represented_var['validPeriod']:
                source_metadata['attributeVariables'][idx]['representedVariables'][idx_repr_var]['validPeriod']['stop'] \
                    = represented_var['validPeriod']['stop']


def _patch_temporal_coverage(source_metadata: dict, destination_metadata: dict):
    source_metadata['temporalCoverage']['start'] = destination_metadata['temporalCoverage']['start']
    if 'stop' in destination_metadata['temporalCoverage']:
        source_metadata['temporalCoverage']['stop'] = destination_metadata['temporalCoverage']['stop']


class PatchMetadataException(Exception):
    pass
