Airflow
---

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

- Clone this repo
- Install the prerequisites
- Run the service
- Check http://localhost:8080
- Done! :tada:

### Prerequisites

- Install [Docker](https://www.docker.com/)
- Install [Docker Compose](https://docs.docker.com/compose/install/)
- Following the Airflow release from [Python Package Index](https://pypi.python.org/pypi/apache-airflow)

### Usage

Run the web service with docker

```
docker-compose up -d

# Build the image
# docker-compose up -d --build
```

Check http://localhost:8080/

- `docker-compose logs` - Displays log output
- `docker-compose ps` - List containers
- `docker-compose down` - Stop containers

## Other commands

If you want to run airflow sub-commands, you can do so like this:

- `docker-compose run --rm webserver airflow list_dags` - List dags
- `docker-compose run --rm webserver airflow test [DAG_ID] [TASK_ID] [EXECUTION_DATE]` - Test specific task

If you want to run/test python script, you can do so like this:
- `docker-compose run --rm webserver python /usr/local/airflow/dags/[PYTHON-FILE].py` - Test python script

## Connect to database

If you want to use Ad hoc query, make sure you've configured connections:
Go to Admin -> Connections and Edit "postgres_default" set this values:
- Host : postgres
- Schema : airflow
- Login : airflow
- Password : airflow


## Usage in K8s

With newest helm chart (https://airflow-helm.github.io/charts), this repo is synced periodically to k8s
Please consider this 2 files:

1. If you make changes in `./dags/helper.py` make sure to also copy it to `./helper.py` (related to airflow helm chart pythonpath location)
2. If you need extra python modules to be installed, also add it to requirements.txt. After that restart all airflow-related pods using
   ```
   kubectl -n default rollout restart  deploy <airflow-release-name>-flower
   kubectl -n default rollout restart  deploy <airflow-release-name>-web
   kubectl -n default rollout restart  deploy <airflow-release-name>-scheduler
   kubectl -n default rollout restart  sts <airflow-release-name>-worker
   ```
## Credits

- [Apache Airflow](https://github.com/apache/incubator-airflow)
- [docker-airflow](https://github.com/puckel/docker-airflow/tree/1.10.0-5)

# Run Docker Airflow in local

If you never this application on your local, you need to setup the environment first. 

`docker-compose up airflow-init`

after the installation is done. you can run this command for setup all the services:

`docker-compose up --build -d`

if you already using this docker, you can just use:

`docker-compose up -d`

This is the url that you need to know:

1. Adminer:
- Host : http://localhost:2398/

2. Postgresql
- System : PostgreSQL
- Host : localhost
- Schema : airflow
- Login : airflow
- Password : airflow
- port : 54341

3. Web Server
- Host : http://localhost:8081/
- Username : airflow
- Password : airflow