In the module_4 folder.

`dbt init --project-dir .`

`dbt deps --project-dir . --profiles-dir .`

`dbt build --project-dir . --profiles-dir .`

`dbt retry --project-dir . --profiles-dir .`

`dbt run --project-dir . --profiles-dir .`

`dbt seed --project-dir . --profiles-dir .`

`dbt docs generate`

`dbt docs serve --port=10000`

`dbt test --project-dir . --profiles-dir .`

`dbt compile --project-dir . --profiles-dir .`

`dbt debug --profiles-dir .`

`dbt show --project-dir . --profiles-dir . --models stg_green_tripdata`
