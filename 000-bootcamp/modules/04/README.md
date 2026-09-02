# Module 4

## Links

- https://docs.getdbt.com/docs/introduction?version=2
- https://docs.getdbt.com/docs/build/projects?version=2
- https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview?version=2
- https://docs.getdbt.com/docs/build/sources?version=2
- https://docs.getdbt.com/docs/build/sql-models?version=2
- https://docs.getdbt.com/reference/dbt-jinja-functions/ref?version=2
- https://docs.getdbt.com/docs/build/seeds?version=2
- https://docs.getdbt.com/docs/build/jinja-macros?version=2
- https://docs.getdbt.com/docs/build/documentation?version=2
- https://docs.getdbt.com/reference/model-properties?version=2
- https://docs.getdbt.com/docs/build/data-tests?version=2
- https://docs.getdbt.com/docs/build/unit-tests?version=2
- https://docs.getdbt.com/docs/mesh/govern/model-contracts?version=2
- https://hub.getdbt.com/


## Exercises

### 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

>>> int_trips_unioned

### 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

>>> [ERROR]: in test accepted_values_fct_trips_payment_type__False__1__2__3__4__5

### 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

```sh
dbt show --project-dir . --profiles-dir . --inline 'SELECT COUNT(pickup_zone) FROM {{ ref("fct_monthly_zone_revenue") }}'
```

>>> 12184

### 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

```sh
dbt show --project-dir . --profiles-dir . --inline "select pickup_zone, sum(revenue_monthly_total_amount) as revenue_total_amount from {{ ref('fct_monthly_zone_revenue') }} WHERE service_type = 'Green' and YEAR(revenue_month) = 2020 GROUP BY pickup_zone ORDER BY revenue_total_amount DESC" --limit 1
```

>>> East Harlem North

### 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

```sh
dbt show --project-dir . --profiles-dir . --inline "select sum(total_monthly_trips) from {{ ref('fct_monthly_zone_revenue') }} WHERE service_type = 'Green' and YEAR(revenue_month) = 2019 and MONTH(revenue_month) = 10 GROUP BY service_type"
```

>>> 384624

### 6. Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

Ingest fhv data:

```sh
python load_and_ingest_fhv.py

dbt show --project-dir . --profiles-dir . --inline "select * from {{ source('raw', 'fhv_tripdata') }}"
```

Command used to generate staging model starting code: `dbt run-operation generate_base_model --args '{"source_name": "raw", "table_name": "fhv_tripdata"}'`

Generate staging model in the data warehouse: `dbt run --project-dir . --profiles-dir . --select stg_fhv_tripdata`

Command used to generate starting shema code for the staging model: `dbt run-operation generate_model_yaml --args '{"model_names": ["stg_fhv_tripdata"]}'`
