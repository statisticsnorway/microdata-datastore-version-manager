import sys
import json
import pika
import logging
from datastore_version_manager import commands
from datastore_version_manager.config import config
from jsonschema import validate

# TODO: fix json logging for elastic
logging.basicConfig(level="INFO")
logger = logging.getLogger()
CONFIG = config.get_config()


def handle_message(channel, method, properties, body):
    logger.info(f'Handling message: {body}')
    body = json.loads(body)
    try:
        with open(CONFIG["JSON_SCHEMA_FILE"], "r") as f:
            schema = json.load(f)
        validate(instance=body, schema=schema)
    except Exception as e:
        logger.error(f"{e.message}")
        return

    # job_id = body['jobId'] use this for logging
    command = body['command']
    parameters = body['parameters']

    if command == "ADD_DATASET":
        commands.add_new_dataset(parameters["datasetName"])
    elif command == "HARD_DELETE":
        commands.hard_delete(parameters["datasetName"])
    elif command == "BUMP_VERSION":
        commands.bump_version(parameters["description"])
    elif command == "SET_STATUS":
        commands.set_status(
            parameters["datasetName"],
            parameters["releaseStatus"],
            parameters["operation"],
            parameters["description"]
        )
    else:
        logger.error(f"Unknown command: {command}. Unable to process.")


def consume_messages():
    credentials = pika.PlainCredentials(
        CONFIG["RABBIT_MQ_USER"], CONFIG["RABBIT_MQ_PASSWORD"]
    )
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            CONFIG["RABBIT_MQ_HOST"],
            CONFIG["RABBIT_MQ_PORT"],
            '/', credentials
        )
    )
    consumer_queue = CONFIG["CONSUMER_QUEUE"]
    channel = connection.channel()
    channel.queue_declare(consumer_queue)
    channel.basic_consume(
        queue=consumer_queue,
        auto_ack=True,
        on_message_callback=handle_message
    )
    logger.info(f"Started to consume from '{consumer_queue}'")
    channel.start_consuming()


if __name__ == '__main__':
    try:
        consume_messages()
    except Exception as e:
        logger.error(f'Application shut down due to {e}')
        sys.exit(0)
