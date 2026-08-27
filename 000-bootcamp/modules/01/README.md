# Homework for Module 1. Simple pipelines.

1. Download dataset
2. Transform and clean
3. Load into PostgreSQL
4. Process data in chunks

## datasets

- [Taxi trips](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet)
- [Zones](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)

## setup

```sh
docker compose up postgres pgadmin
```

## Exercises

### For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?

```sh
uv run python pipeline_01.py
```

### Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).

```sh
uv run python pipeline_02.py
```

### Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

```sh
uv run python pipeline_03.py
```

### For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

```sh
uv run python pipeline_04.py
```
