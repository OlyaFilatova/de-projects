# Module 3 Homework

For this homework we will be using the Yellow Taxi Trip Records for January 2024 - June 2024 (not the entire year of data).

Parquet Files are available from the New York City Taxi Data found here:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Diff

I'm using DuckDB instead of GCS and BigQuery.

Skipping exercises of type "estimated bytes" (tied to BigQuery UI).

## Exercises

### 0. Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records.

- 0_homework_prep

### 1. What is count of records for the 2024 Yellow Taxi Data?

- 1_homework_1. Task count_rows_2024.

### 2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

- 1_homework_1. Task pu_location_distinct.

### 3. Write a query to retrieve the PULocationID from the table. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

- 1_homework_1. Task retreive_locations.

### 4. How many records have a fare_amount of 0?

- 1_homework_1. Task empty_fare_count.

### 5. What is the best strategy to make an optimized table if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

- 1_homework_1. Task optimized_table.

### 6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

- 1_homework_1. Task distinct_vendor_ids.
