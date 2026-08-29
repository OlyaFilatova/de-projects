from pathlib import Path
import shutil
from typing import Any

from airflow.sdk import dag, task
import duckdb

from lib.taxi_duckdb import DUCKDB_PATH, TABLE_YELLOW_TRIPS, TABLESET_YELLOW_TRIPS_PARTITIONED


@dag(
  dag_id="1_homework",
  schedule=None,
  catchup=False,
  tags=["nyc-taxi", "postgres", "etl"],
)
def homework():
  @task
  def count_rows_2024() -> int:
    """
    1. What is count of records 
    for the 2024 Yellow Taxi Data?
    """
    with duckdb.connect(DUCKDB_PATH) as conn:

      count = conn.execute(
        f"""
        SELECT
          COUNT(VendorID)
        FROM {TABLE_YELLOW_TRIPS}
        """,
      ).fetchone()[0]

      print(f"There are {count} records for the 2024 Yellow Taxi Data.")

      return count

  @task
  def pu_location_distinct() -> int:
    """
    2. Write a query to count 
    the distinct number of PULocationIDs 
    for the entire dataset.
    """
    with duckdb.connect(DUCKDB_PATH) as conn:
      count = conn.execute(
        f"""
        SELECT
          COUNT(DISTINCT PULocationID)
        FROM {TABLE_YELLOW_TRIPS}
        """,
      ).fetchone()[0]

      print(f"There are {count} distinct PULocationIDs.")

      return count

  @task
  def retreive_locations() -> None:
    """
    3. Write a query to retrieve 
    the PULocationID from the table. 
    Now write a query to retrieve the PULocationID 
    and DOLocationID on the same table.
    """
    with duckdb.connect(DUCKDB_PATH) as conn:
      print('Connected to DB')
      pu_locations = conn.execute(
        f"""
        SELECT
           PULocationID
        FROM {TABLE_YELLOW_TRIPS}
        """,
      ).fetchall()

      print(f"Retrieved PULocationIDs of {len(pu_locations)} rows")

      locations = conn.execute(
        f"""
        SELECT
           PULocationID, DOLocationID
        FROM {TABLE_YELLOW_TRIPS}
        """,
      ).fetchall()

      print(f"PULocationID and DOLocationID pairs of {len(locations)} rows")


  @task
  def empty_fare_count() -> int:
    """ 4. How many records have a fare_amount of 0? """

    with duckdb.connect(DUCKDB_PATH) as conn:

      count = conn.execute(
        f"""
        SELECT
          COUNT(fare_amount)
        FROM {TABLE_YELLOW_TRIPS}
        WHERE fare_amount = 0
        """,
      ).fetchone()[0]

      print(f"There are {count} records with fare_amount equal 0.")

      return count

  @task
  def optimized_table():
    """
    5. What is the best strategy to make 
    an optimized table in Big Query 
    if your query will always filter 
    based on tpep_dropoff_datetime and 
    order the results by VendorID?
    """
    output_path = Path(TABLESET_YELLOW_TRIPS_PARTITIONED)

    if output_path.exists():
        shutil.rmtree(output_path)
        
    with duckdb.connect(DUCKDB_PATH) as conn:
      print("Connected to DuckDB")
      conn.execute("SET memory_limit='2GB'")
      conn.execute("SET temp_directory='/tmp/duckdb_tmp'")
      print("Setup memory limit")
      conn.execute(
        f"""
        COPY (
          SELECT *, DATE(tpep_dropoff_datetime) AS dropoff_date
          FROM {TABLE_YELLOW_TRIPS}
          ORDER BY dropoff_date, VendorID
        )
        TO '{TABLESET_YELLOW_TRIPS_PARTITIONED}'
        (
          FORMAT PARQUET,
          PARTITION_BY (dropoff_date)
        );
        """
      )
      print("Partitioned table")

      count1 = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {TABLE_YELLOW_TRIPS}
        WHERE tpep_dropoff_datetime >= '2024-01-01' AND 
        tpep_dropoff_datetime < '2024-02-01'
        """,
      ).fetchone()[0]
      print(f"Queried table: {count1}")

      count2 = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{TABLESET_YELLOW_TRIPS_PARTITIONED}/**/*.parquet')
        WHERE tpep_dropoff_datetime >= '2024-01-01' AND 
        tpep_dropoff_datetime < '2024-02-01'
        """,
      ).fetchone()[0]
      print(f"Queried partitioned table: {count2}")

  @task
  def distinct_vendor_ids() -> list[tuple[Any, ...]]:
    """
    6. Write a query to 
    retrieve the distinct VendorIDs 
    between tpep_dropoff_datetime 
    2024-03-01 and 2024-03-15 (inclusive)
    """
    with duckdb.connect(DUCKDB_PATH) as conn:
      vendor_ids = conn.execute(
        f"""
        SELECT DISTINCT VendorID
        FROM read_parquet('{TABLESET_YELLOW_TRIPS_PARTITIONED}/**/*.parquet')
        WHERE tpep_dropoff_datetime >= '2024-03-01' AND 
        tpep_dropoff_datetime <= '2024-03-15'
        """,
      ).fetchall()

      print(f"Distinct Vendor ids: {vendor_ids}.")

      return vendor_ids

  @task
  def distinct_vendor_ids_unoptimized() -> list[tuple[Any, ...]]:
    """
    retrieve the distinct VendorIDs 
    between tpep_dropoff_datetime 
    2024-03-01 and 2024-03-15 (inclusive)
    from table that was not partitioned
    """
    with duckdb.connect(DUCKDB_PATH) as conn:
      vendor_ids = conn.execute(
        f"""
        SELECT DISTINCT VendorID
        FROM {TABLE_YELLOW_TRIPS}
        WHERE tpep_dropoff_datetime >= '2024-03-01' AND 
        tpep_dropoff_datetime <= '2024-03-15'
        """,
      ).fetchall()

      print(f"Distinct Vendor ids: {vendor_ids}.")

      return vendor_ids

  count_rows_2024() >> pu_location_distinct() >> retreive_locations() \
    >> empty_fare_count() >> optimized_table() >> distinct_vendor_ids() \
    >> distinct_vendor_ids_unoptimized()

homework()
