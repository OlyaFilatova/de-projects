from __future__ import annotations

import hashlib
import gzip
import shutil
import tempfile
from pathlib import Path

import requests

from airflow.sdk import dag, get_current_context, task, Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime


POSTGRES_CONN_ID = "homework_postgres"

BASE_URL = (
  "https://github.com/DataTalksClub/nyc-tlc-data/"
  "releases/download/{taxi}/{filename}.gz"
)


TAXI_CONFIG = {
  "yellow": {
    "datetime_columns": [
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
    ],
    "columns": [
      "VendorID",
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "passenger_count",
      "trip_distance",
      "RatecodeID",
      "store_and_fwd_flag",
      "PULocationID",
      "DOLocationID",
      "payment_type",
      "fare_amount",
      "extra",
      "mta_tax",
      "tip_amount",
      "tolls_amount",
      "improvement_surcharge",
      "total_amount",
      "congestion_surcharge",
    ],
    "schema": """
      unique_row_id          text,
      filename               text,
      VendorID               text,
      tpep_pickup_datetime   timestamp,
      tpep_dropoff_datetime  timestamp,
      passenger_count        integer,
      trip_distance          double precision,
      RatecodeID             text,
      store_and_fwd_flag     text,
      PULocationID           text,
      DOLocationID           text,
      payment_type           integer,
      fare_amount            double precision,
      extra                  double precision,
      mta_tax                double precision,
      tip_amount             double precision,
      tolls_amount           double precision,
      improvement_surcharge  double precision,
      total_amount           double precision,
      congestion_surcharge  double precision
    """,
  },
  "green": {
    "datetime_columns": [
      "lpep_pickup_datetime",
      "lpep_dropoff_datetime",
    ],
    "columns": [
      "VendorID",
      "lpep_pickup_datetime",
      "lpep_dropoff_datetime",
      "store_and_fwd_flag",
      "RatecodeID",
      "PULocationID",
      "DOLocationID",
      "passenger_count",
      "trip_distance",
      "fare_amount",
      "extra",
      "mta_tax",
      "tip_amount",
      "tolls_amount",
      "ehail_fee",
      "improvement_surcharge",
      "total_amount",
      "payment_type",
      "trip_type",
      "congestion_surcharge",
    ],
    "schema": """
      unique_row_id          text,
      filename               text,
      VendorID               text,
      lpep_pickup_datetime   timestamp,
      lpep_dropoff_datetime  timestamp,
      store_and_fwd_flag     text,
      RatecodeID             text,
      PULocationID           text,
      DOLocationID           text,
      passenger_count        integer,
      trip_distance          double precision,
      fare_amount            double precision,
      extra                  double precision,
      mta_tax                double precision,
      tip_amount             double precision,
      tolls_amount           double precision,
      ehail_fee              double precision,
      improvement_surcharge  double precision,
      total_amount           double precision,
      payment_type           integer,
      trip_type              integer,
      congestion_surcharge  double precision
    """,
  },
}


def make_unique_row_id(row: dict, taxi: str) -> str:
  """Generate the logical hash."""

  config = TAXI_CONFIG[taxi]
  datetime_columns = config["datetime_columns"]

  values = [
    row.get("VendorID") or "",
    row.get(datetime_columns[0]) or "",
    row.get(datetime_columns[1]) or "",
    row.get("PULocationID") or "",
    row.get("DOLocationID") or "",
    row.get("fare_amount") or "",
    row.get("trip_distance") or "",
  ]

  raw_value = "".join(str(value) for value in values)

  return hashlib.md5(raw_value.encode()).hexdigest()

def create_taxi_dag(dag_id: str, cron: str, taxi: str):
  @dag(
    dag_id=dag_id,
    start_date=datetime(2024, 1, 1),
    schedule=cron,
    catchup=False,
    tags=["nyc-taxi", "postgres", "etl"],
    params={
      "taxi": Param(
        taxi,
        type="string",
        enum=["yellow", "green"],
        title="Taxi type",
      ),
    },
  )
  def nyc_taxi_to_postgres():


    @task
    def build_config(params=None) -> dict:
      taxi = params["taxi"]

      context = get_current_context()
      date = context["logical_date"].strftime("%Y-%m")

      filename = f"{taxi}_tripdata_{date}.csv"

      return {
        "taxi": taxi,
        "date": date,
        "filename": filename,
        "url": BASE_URL.format(
          taxi=taxi,
          filename=filename,
        ),
        "table": f"public.{taxi}_tripdata",
        "staging_table": f"public.{taxi}_tripdata_staging",
      }

    @task
    def download(config: dict) -> dict:
      """
      Download and decompress the TLC file.
      """

      temp_dir = Path(tempfile.mkdtemp(prefix="nyc_taxi_"))
      compressed_file = temp_dir / f"{config['filename']}.gz"
      csv_file = temp_dir / config["filename"]

      response = requests.get(
        config["url"],
        stream=True,
        timeout=300,
      )
      response.raise_for_status()

      with compressed_file.open("wb") as output:
        shutil.copyfileobj(response.raw, output)

      with gzip.open(compressed_file, "rb") as source:
        with csv_file.open("wb") as target:
          shutil.copyfileobj(source, target)

      compressed_file.unlink()

      return {
        **config,
        "local_file": str(csv_file),
      }

    @task
    def load_to_postgres(config: dict) -> None:
      """
      Create tables, load CSV into staging, generate IDs and merge
      into the target table.
      """

      taxi = config["taxi"]
      table = config["table"]
      staging_table = config["staging_table"]
      filename = config["filename"]
      csv_file = config["local_file"]

      taxi_config = TAXI_CONFIG[taxi]
      columns = taxi_config["columns"]
      schema = taxi_config["schema"]

      hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID,
      )

      # ------------------------------------------------------------------
      # 1. Create target and staging tables
      # ------------------------------------------------------------------

      create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
          {schema}
        );

        CREATE TABLE IF NOT EXISTS {staging_table} (
          {schema}
        );
      """

      hook.run(create_table_sql)

      # ------------------------------------------------------------------
      # 2. Clear staging table
      # ------------------------------------------------------------------

      hook.run(f"TRUNCATE TABLE {staging_table}")

      # ------------------------------------------------------------------
      # 3. COPY CSV -> staging
      # ------------------------------------------------------------------

      conn = hook.get_conn()

      copy_sql = f"""
        COPY {staging_table}
        ({", ".join(columns)})
        FROM STDIN
        WITH (
          FORMAT CSV,
          HEADER TRUE
        )
      """
      with conn.cursor() as cursor:
        with cursor.copy(copy_sql) as copy:
          with open(csv_file, "rb") as csv_file_handle:
            while data := csv_file_handle.read(1024 * 1024):
              copy.write(data)

      conn.commit()

      # ------------------------------------------------------------------
      # 4. Generate unique_row_id + filename
      # ------------------------------------------------------------------

      datetime_columns = taxi_config["datetime_columns"]

      update_sql = f"""
        UPDATE {staging_table}
        SET
          unique_row_id = md5(
            COALESCE(CAST(VendorID AS text), '') ||
            COALESCE(CAST({datetime_columns[0]} AS text), '') ||
            COALESCE(CAST({datetime_columns[1]} AS text), '') ||
            COALESCE(PULocationID, '') ||
            COALESCE(DOLocationID, '') ||
            COALESCE(CAST(fare_amount AS text), '') ||
            COALESCE(CAST(trip_distance AS text), '')
          ),
          filename = %s
      """

      hook.run(
        update_sql,
        parameters=(filename,),
      )

      # ------------------------------------------------------------------
      # 5. Merge staging -> target
      # ------------------------------------------------------------------

      insert_columns = [
        "unique_row_id",
        "filename",
        *columns,
      ]

      column_list = ", ".join(insert_columns)
      source_columns = ", ".join(
        f"S.{column}" for column in insert_columns
      )

      merge_sql = f"""
        MERGE INTO {table} AS T
        USING {staging_table} AS S
        ON T.unique_row_id = S.unique_row_id

        WHEN NOT MATCHED THEN
          INSERT (
            {column_list}
          )
          VALUES (
            {source_columns}
          );
      """

      hook.run(merge_sql)

      # ------------------------------------------------------------------
      # 6. Remove temporary file
      # ------------------------------------------------------------------

      Path(config["local_file"]).unlink(missing_ok=True)
      Path(config["local_file"]).parent.rmdir()

    config = build_config()
    downloaded_file = download(config)
    load_to_postgres(downloaded_file)

  nyc_taxi_to_postgres()

green_dag = create_taxi_dag(
    dag_id="05_green_schedule",
    cron="0 9 1 * *",
    taxi="green",
)

yellow_dag = create_taxi_dag(
    dag_id="05_yellow_schedule",
    cron="0 10 1 * *",
    taxi="yellow",
)
