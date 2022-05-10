
def patch(source_metadata: dict, destination_metadata: dict) -> dict:
    """
    Will only patch selected fields from source_metadata object to destination_metadata object.
    :param source_metadata: Metadata of a dataset provided by user with patched fields
    :param destination_metadata: The latest versioned metadata of a dataset from datastore
    :return:
    """
    patched = {
        'populationDescription': source_metadata['populationDescription']
    }
    destination_metadata.update(patched)

    return destination_metadata
