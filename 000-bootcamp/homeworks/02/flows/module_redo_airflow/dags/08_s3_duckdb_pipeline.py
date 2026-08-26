from __future__ import annotations

import gzip
from pathlib import Path

import duckdb
import requests
from airflow.sdk import dag, task, Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime


BUCKET_NAME = "bucket1"
BUCKET_REGION = "eu-central-1"
AWS_CONN_ID = "localstack"
AWS_ENDPOINT = "localhost.localstack.cloud"

DUCKDB_PATH = "/tmp/tripdata.duckdb"

DATA_URL = (
  "https://github.com/DataTalksClub/nyc-tlc-data"
  "/releases/download/{taxi}/{file}.gz"
)


TAXI_CONFIG = {
  "yellow": {
    "table": "yellow_tripdata",
    "pickup": "tpep_pickup_datetime",
    "dropoff": "tpep_dropoff_datetime",
    "schema": """
      unique_row_id VARCHAR,
      filename VARCHAR,
      VendorID VARCHAR,
      tpep_pickup_datetime TIMESTAMP,
      tpep_dropoff_datetime TIMESTAMP,
      passenger_count INTEGER,
      trip_distance DECIMAL(18,3),
      RatecodeID VARCHAR,
      store_and_fwd_flag VARCHAR,
      PULocationID VARCHAR,
      DOLocationID VARCHAR,
      payment_type INTEGER,
      fare_amount DECIMAL(18,3),
      extra DECIMAL(18,3),
      mta_tax DECIMAL(18,3),
      tip_amount DECIMAL(18,3),
      tolls_amount DECIMAL(18,3),
      improvement_surcharge DECIMAL(18,3),
      total_amount DECIMAL(18,3),
      congestion_surcharge DECIMAL(18,3)
    """,
  },
  "green": {
    "table": "green_tripdata",
    "pickup": "lpep_pickup_datetime",
    "dropoff": "lpep_dropoff_datetime",
    "schema": """
      unique_row_id VARCHAR,
      filename VARCHAR,
      VendorID VARCHAR,
      lpep_pickup_datetime TIMESTAMP,
      lpep_dropoff_datetime TIMESTAMP,
      store_and_fwd_flag VARCHAR,
      RatecodeID VARCHAR,
      PULocationID VARCHAR,
      DOLocationID VARCHAR,
      passenger_count INTEGER,
      trip_distance DECIMAL(18,3),
      fare_amount DECIMAL(18,3),
      extra DECIMAL(18,3),
      mta_tax DECIMAL(18,3),
      tip_amount DECIMAL(18,3),
      tolls_amount DECIMAL(18,3),
      ehail_fee DECIMAL(18,3),
      improvement_surcharge DECIMAL(18,3),
      total_amount DECIMAL(18,3),
      payment_type INTEGER,
      trip_type INTEGER,
      congestion_surcharge DECIMAL(18,3)
    """,
  },
}


@dag(
  dag_id="08_s3_duckdb_pipeline",
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  tags=["nyc-taxi", "s3", "duckdb", "etl"],
  params={
    "taxi": Param(
      "yellow",
      type="string",
      enum=["yellow", "green"],
    ),
    "year": Param(
      "2019",
      type="string",
      enum=["2019", "2020"],
    ),
    "month": Param(
      "01",
      type="string",
      enum=[
        "01", "02", "03", "04", "05", "06",
        "07", "08", "09", "10", "11", "12",
      ],
    ),
  },
)
def s3_duckdb_pipeline():

  @task
  def extract_and_upload(
    taxi: str,
    year: str,
    month: str,
  ) -> str:
    file = f"{taxi}_tripdata_{year}-{month}.csv"

    url = DATA_URL.format(
      taxi=taxi,
      file=file,
    )

    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

    # Temporary local file only for the upload.
    # It is NOT downloaded again after uploading.
    local_path = Path("/tmp") / file

    with gzip.GzipFile(fileobj=response.raw) as gz:
      with local_path.open("wb") as output:
        while chunk := gz.read(1024 * 1024):
          output.write(chunk)

    s3.load_file(
      filename=str(local_path),
      key=file,
      bucket_name=BUCKET_NAME,
      replace=True,
    )

    local_path.unlink()

    return f"s3://{BUCKET_NAME}/{file}"

  @task
  def load_to_duckdb(
    s3_path: str,
    taxi: str,
    year: str,
    month: str,
  ):
    config = TAXI_CONFIG[taxi]

    table = config["table"]
    pickup = config["pickup"]
    dropoff = config["dropoff"]

    conn = duckdb.connect(DUCKDB_PATH)

    try:
      conn.execute("INSTALL httpfs")
      conn.execute("LOAD httpfs")

      # LocalStack S3 configuration.
      conn.execute(f"""
        SET s3_region = '{BUCKET_REGION}';
        SET s3_endpoint = '{AWS_ENDPOINT}:4566';
        SET s3_use_ssl = false;
        SET s3_url_style = 'path';
        SET s3_access_key_id = 'test';
        SET s3_secret_access_key = 'test';
      """)

      conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          {config["schema"]}
        )
        """
      )

      stage_table = f"{table}_stage"

      conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {stage_table} AS
        SELECT
          md5(
            concat(
              coalesce(CAST(VendorID AS VARCHAR), ''),
              coalesce(CAST({pickup} AS VARCHAR), ''),
              coalesce(CAST({dropoff} AS VARCHAR), ''),
              coalesce(CAST(PULocationID AS VARCHAR), ''),
              coalesce(CAST(DOLocationID AS VARCHAR), '')
            )
          ) AS unique_row_id,
          ? AS filename,
          *
        FROM read_csv_auto(
          ?,
          header = true,
          auto_detect = true,
          ignore_errors = true
        )
        """,
        [
          f"{taxi}_tripdata_{year}-{month}.csv",
          s3_path,
        ],
      )

      conn.execute(
        f"""
        INSERT INTO {table}
        SELECT s.*
        FROM {stage_table} s
        WHERE NOT EXISTS (
          SELECT 1
          FROM {table} t
          WHERE t.unique_row_id = s.unique_row_id
        )
        """
      )

    finally:
      conn.close()

  taxi = "{{ params.taxi }}"
  year = "{{ params.year }}"
  month = "{{ params.month }}"

  s3_path = extract_and_upload(
    taxi=taxi,
    year=year,
    month=month,
  )

  load_to_duckdb(
    s3_path=s3_path,
    taxi=taxi,
    year=year,
    month=month,
  )

s3_duckdb_pipeline()
