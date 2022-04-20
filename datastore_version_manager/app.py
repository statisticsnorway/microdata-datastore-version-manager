import uuid
import logging
import json_logging

from flask import Flask
from datastore_version_manager.api.command_api import command_api
from datastore_version_manager.config.logging import (
    CustomJSONLog, CustomJSONRequestLogFormatter
)
from datastore_version_manager.exceptions.exceptions import ForbiddenOperation


def init_json_logging():
    json_logging.CREATE_CORRELATION_ID_IF_NOT_EXISTS = True
    json_logging.CORRELATION_ID_GENERATOR = (
        lambda: "job-service-" + str(uuid.uuid1())
    )
    json_logging.init_flask(
        enable_json=True,
        custom_formatter=CustomJSONLog
    )
    json_logging.init_request_instrument(
        app, custom_formatter=CustomJSONRequestLogFormatter
    )


logging.getLogger("json_logging").setLevel(logging.WARNING)

app = Flask(__name__)
app.register_blueprint(command_api)


@app.errorhandler(ForbiddenOperation)
def handle_not_found(e):
    return {'message': f'BAD REQUEST: {str(e)}'}, 400
