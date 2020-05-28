# METROBUS_PULL_DATA

This app pull data about "metrobus" (yo need to configure teh url) of México City every hour.

## DEPENDENCIES

- python3.8.3
- list of dependencies comes in requirements.txt
- a database postgres where you get the data (you need to configure the database, otherwise I sugest use the code of [metrobus api](https://github.com/siht/metrobus_api) it has the tables and info that you need in order to this app works well)

### ENVIRONMENT VARIABLES

- DB_NAME
- DB_USER
- DB_PASSWORD
- DB_HOST
- DB_PORT
- BROKER
- METROBUSES_API_URL: the value is (https://datos.cdmx.gob.mx/api/records/1.0/search/?-dataset=prueba_fetchdata_metrobus&q=)

can you set with a .env file, only put that file unde rest_project directory, [read why](https://pypi.org/project/python-dotenv/).

## HOW TO INSTALL

run inside this project

```sh
pip install -r requirements.txt
```

for install dependencies

set all environment variables

do the setup for a postgres database with [metrobus api](https://github.com/siht/metrobus_api)

## HOW TO RUN

### DEVELOPMENT

after that you have installed and configured only left run

first in one console

```sh
celery -A pull_data worker -l info
```

and in other console

```sh
celery -A pull_data beat -l info
```

### PRODUCTION

because you need to manage two processes you need supervisor to run both.
