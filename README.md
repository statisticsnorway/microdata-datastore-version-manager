# microdata-datastore-version-manager

Data store version manager for microdata.no.


## Contribute

### Set up
To work on this repository you need to install [poetry](https://python-poetry.org/docs/):
```
# macOS / linux / BashOnWindows
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -

# Windows powershell
(Invoke-WebRequest -Uri https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py -UseBasicParsing).Content | python -
```
Then install the virtual environment from the root directory:
```
poetry install
```

### Running without Docker
You should add the appropriate environmental variables to your local system:
```sh
export DATASET_BUILDER_OUTPUT_DIR=/output
export DATASTORE_ROOT_DIR=/datastore
```

Open terminal and go to root directory of the project and run:
````
poetry run python datastore_version_manager/commands.py TODO arguments here
````

### Install dependencies:
If you do not wish to run the service in a virtual environment, install the project dependencies by exporting them from poetry:
```  
poetry export > requirements.txt
pip install -r requirements.txt
```

### Build Docker image
````
docker build --tag datastore-version-manager:local-latest .
````

### Running Docker image
Edit `docker-compose.yml` file and change environment and volumes sections as necessary.
Make sure that data and metadata files are present in input directory and that you are using
the correct tag of datastore-builder image.
Then run:
````
docker-compose run datastore-version-manager TODO arguments here
````

### Running tests
Open terminal and go to root directory of the project and run:
````
poetry run pytest --cov=datastore_version_manager/
````
