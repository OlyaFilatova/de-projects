import duckdb

from config.taxi import TAXI_DUCKDB_CONFIG
from config.s3 import BUCKET_REGION, AWS_ENDPOINT


DUCKDB_PATH = "/tmp/tripdata.duckdb"


def configure_s3(conn) -> None:
  conn.execute("INSTALL httpfs")
  conn.execute("LOAD httpfs")

  conn.execute(f"""
    SET s3_region = '{BUCKET_REGION}';
    SET s3_endpoint = '{AWS_ENDPOINT}:4566';
    SET s3_use_ssl = false;
    SET s3_url_style = 'path';
    SET s3_access_key_id = 'test';
    SET s3_secret_access_key = 'test';
  """)

def create_table(
  conn,
  *,
  table: str,
  schema: str,
) -> None:

  conn.execute(
    f"""
    CREATE TABLE IF NOT EXISTS {table} (
      {schema}
    )
    """
  )

def stage_data(
  conn,
  *,
  table: str,
  pickup: str,
  dropoff: str,
  s3_path: str,
  filename: str,
) -> None:

  stage_table = f"{table}_stage"

  conn.execute(
    f"""
    CREATE OR REPLACE TEMP TABLE {stage_table} AS
    SELECT
      md5(
        concat_ws(
          '|',
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
      filename,
      s3_path,
    ],
  )

def merge_data(
    conn,
    *,
    table: str,
) -> None:

  stage_table = f"{table}_stage"

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

def load_from_s3(
  *,
  s3_path: str,
  taxi: str,
  filename: str,
) -> None:

  config = TAXI_DUCKDB_CONFIG[taxi]

  conn = duckdb.connect(DUCKDB_PATH)

  try:
    configure_s3(conn)

    create_table(
      conn,
      table=config.table,
      schema=config.schema,
    )

    stage_data(
      conn,
      table=config.table,
      pickup=config.pickup,
      dropoff=config.dropoff,
      s3_path=s3_path,
      filename=filename,
    )

    merge_data(
      conn,
      table=config.table,
    )

  finally:
    conn.close()
