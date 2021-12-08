# Export Poetry Packages
FROM ubuntu:20.10 as builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.1.4 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

# Prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

# Install python 3.9
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    python3.9 \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3.9 /usr/bin/python

# Install tools
RUN apt-get update \
    && apt-get install -y  --no-install-recommends \
    ca-certificates \
    curl \
    build-essential \
    python3-distutils \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY poetry.lock pyproject.toml /app/

# Install poetry and export dep endencies to requirements yaml
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python -
RUN poetry export > requirements.txt

# Production image
FROM python:3.9-slim-bullseye

WORKDIR /app
COPY datastore_version_manager datastore_version_manager
COPY --from=builder /app/requirements.txt requirements.txt

RUN pip install -r requirements.txt

#the output is sent straight to terminal without being first buffered
ENV PYTHONUNBUFFERED 1

ENV PYTHONPATH "/app/datastore_version_manager/:$PYTHONPATH"

ENTRYPOINT [ "python", "datastore_version_manager/commands.py" ]

