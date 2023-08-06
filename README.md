# Overview

An ETL process that discovers top skills required by vacancies for middle python developer provided by telecom companies and built on top of Airflow

NOTE: that any vacancies from unstrusted companies or vacancies that is already in archive are ignored

# Basic requirements

Atleast Docker version 20.10.22, build 3a2c30b

Docker Compose version v2.15.1

The following requirements will builtin in airflow image

* pyyaml==6.0
* SQLAlchemy==1.4.49
* beautifulsoup4==4.12.2
* aiohttp==3.8.3
* pyyaml==6.0
* orjson==3.8.3
* typing_extensions==4.4.0
* SQLAlchemy-Utils==0.39.0

# Setup

To run an airflow and underlying ETL process simple type `docker-compose up -d`

Airflow fronted is accessible via `localhost:8099`

Postgres database is accessible via default port `5432`

Default credentials for airflow and database is `airflow:airflow`

# Configuration

The company [registry file](https://ofdata.ru/open-data/download/egrul.json.zip) is downloaded in `/opt/airflow/external-data` by the default 

In order to allow DAG to access datastore u must define the following postgres connections:

* companies_store
* hh_vacancies_store

Specify host as the name of the service, defined in docker-compose yml (by default postgres), port `5432`

Companies producer configuration located at `/opt/airflow/config`

<details>

<summary>Default configuration</summary>

```yaml
processes:
  egrul-registry:
    disabled: false
    datasource:
      data-path: /opt/airflow/external-data
    datastore:
      connection-url: postgresql://airflow:airflow@postgres:5432/companies
      driver: PostgreDatastore
      parameters:
        batch:
          max-size: 5000
        connection: {}
        init-storage-sql: "TRUNCATE public.company RESTART IDENTITY;"
        max-pool-size: 4
    processor:
      max-io-workers: 2
      max-processes: 8
      transformer:
        parameters:
          model: Company
        transformer: GenericTransformer
```

</details>

HH crawler configuration located at `/opt/airflow/config`

<details>

<summary>Default configuration</summary>

```yaml
datastore:
  connection-string: postgresql://airflow:airflow@postgres:5432/hh_vacancies

vacancies-limit: 1000
vacancies-prefetch: 50
hh-api-endpoint: "https://api.hh.ru/vacancies"
request-timeout-in-seconds: 15
```

</details>

# Results

By the default a skill rating file is located at external-data folder and u can override this by defining variable `skill_rating_output_file` via Admin -> Variables menu

Here is an example

| skill | total vacancies |
| ----- | --------------- |
| python | 3 |
| redis | 2 |
| rabbitmq | 2 |
| flask | 1 |
| kubernetes | 1 |
| postman | 1 |
| pytest | 1 |
| qa | 1 |
| rest | 1 |
| rest api | 1 |
| selenuim | 1 |
| soap | 1 |
| ci/cd | 1 |
| swagger | 1 |
| django framework | 1 |
| docker | 1 |