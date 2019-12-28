# review-pipeline
Pipeline processing reviews based on docker, airflow, dbt and postgres 

## Prerequisites

Requires git, docker and docker-compose to be installed.

## How to run

```bash
> git clone https://github.com/lheres/review-pipeline.git
> cd ./review-pipeline
> docker-compose -f docker-compose-project.yml up
```

The docker-compose script will start 4 containers for airflow, airflow backend (postgres), data warehouse (postgres_dw) and dbt.

The Airflow interface is available at http://localhost:8080

The initial_model_DAG can be enabled and triggered from Airflow to populate a sample database with 1000 reviews and 1000 metadata records.

To populate reviews and metadata from other locations (local or e.g. S3), provide two variables to Airflow with names review_location and metadata_location. See examples in data/variables.json

The postgres data warehouse can be accessed through the following connection:

```
postgres+psycopg2://de:takeitaway@localhost:5430/de
```
