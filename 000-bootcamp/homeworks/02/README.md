## Module follow (Kestra)

`docker compose -f docker-compose.module.yml up`

```sh
curl -X POST -u 'admin@kestra.io:Admin1234!' http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/module_follow/01_hello_world.yaml
curl -X POST -u 'admin@kestra.io:Admin1234!' http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/module_follow/02_python.yaml
curl -X POST -u 'admin@kestra.io:Admin1234!' http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/module_follow/03_data_pipeline.yaml
curl -X POST -u 'admin@kestra.io:Admin1234!' http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/module_follow/04_postgres.yaml
curl -X POST -u 'admin@kestra.io:Admin1234!' http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/module_follow/05_scheduled.yaml
curl -X POST -u 'admin@kestra.io:Admin1234!' http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/module_follow/06_key_value.yaml
curl -X POST -u 'admin@kestra.io:Admin1234!' http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/module_follow/07_s3_create_bucket.yaml
curl -X POST -u 'admin@kestra.io:Admin1234!' http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/module_follow/08_s3_duckdb_pipeline.yaml
curl -X POST -u 'admin@kestra.io:Admin1234!' http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/module_follow/09_llm.yaml
curl -X POST -u 'admin@kestra.io:Admin1234!' http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/module_follow/10_llm_with_rag.yaml
```

## Module 2 Homework

`docker compose -f docker-compose.homework.yml up`

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

(`https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/`)

## Exercises

(?Kestra)

### Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?

### What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?

### How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?

### How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?

### How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?

### How would you configure the timezone to New York in a Schedule trigger?


## Useful commands

```sh
docker compose -f docker-compose.homework.yml exec dezm2h_airflow-scheduler airflow dags list-import-errors
```
