
import os
import sys
import json
import pika
import logging
from datastore_version_manager import commands
from jsonschema import validate

# TODO: fix json logging for elastic
logging.basicConfig(level="INFO")
logger = logging.getLogger()

RABBIT_MQ_USER = os.environ["RABBIT_MQ_USER"]
RABBIT_MQ_PASSWORD = os.environ["RABBIT_MQ_PASSWORD"]
RABBIT_MQ_HOST = os.environ["RABBIT_MQ_HOST"]
RABBIT_MQ_PORT = 5672
CONSUMER_QUEUE = os.environ["CONSUMER_QUEUE"]
JSON_SCHEMA_FILE = "datastore_version_manager/message_schema.json"


def handle_message(channel, method, properties, body):
    logger.info(f'Handling message: {body}')
    body = json.loads(body)
    try:
        with open(JSON_SCHEMA_FILE, mode="r") as f:
            schema = json.load(f)
        validate(instance=body, schema=schema)
    except Exception as e:
        logger.error(f"{e}")
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
    credentials = pika.PlainCredentials(RABBIT_MQ_USER, RABBIT_MQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            RABBIT_MQ_HOST, RABBIT_MQ_PORT, '/', credentials
        )
    )
    channel = connection.channel()
    channel.queue_declare(CONSUMER_QUEUE)
    channel.basic_consume(
        queue=CONSUMER_QUEUE,
        auto_ack=True,
        on_message_callback=handle_message
    )
    logger.info(f"Started to consume from '{CONSUMER_QUEUE}'")
    channel.start_consuming()


if __name__ == '__main__':
    try:
        consume_messages()
    except Exception as e:
        logger.error(f'Application shut down due to {e}')
        sys.exit(0)