import os


def get_config() -> dict:
    try:
        return {
            "RABBIT_MQ_USER": os.environ["RABBIT_MQ_USER"],
            "RABBIT_MQ_PASSWORD": os.environ["RABBIT_MQ_PASSWORD"],
            "RABBIT_MQ_HOST": os.environ["RABBIT_MQ_HOST"],
            "RABBIT_MQ_PORT": os.environ["RABBIT_MQ_PORT"],
            "CONSUMER_QUEUE": os.environ["CONSUMER_QUEUE"],
            "JSON_SCHEMA_FILE ": (
                "datastore_version_manager/message_schema.json"
            )
        }
    except Exception as e:
        raise ConfigError(
            f'Error occured when getting config from environment: {e}'
        )


class ConfigError(Exception):
    pass
