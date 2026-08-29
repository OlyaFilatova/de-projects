import duckdb

DUCKDB_PATH = "/tmp/tripdata.duckdb"

TABLE_YELLOW_TRIPS = 'yellow_trips'
TABLESET_YELLOW_TRIPS_PARTITIONED = f'/tmp/{TABLE_YELLOW_TRIPS}_partitioned'
SCHEMA_YELLOW_TRIPS = """
  unique_row_id text,
  filename text,
  VendorID text,
  tpep_pickup_datetime timestamp,
  tpep_dropoff_datetime timestamp,
  passenger_count integer,
  trip_distance double precision,
  RatecodeID text,
  store_and_fwd_flag text,
  PULocationID text,
  DOLocationID text,
  payment_type integer,
  fare_amount double precision,
  extra double precision,
  mta_tax double precision,
  tip_amount double precision,
  tolls_amount double precision,
  improvement_surcharge double precision,
  total_amount double precision,
  congestion_surcharge double precision
"""
COLUMNS_YELLOW_TRIPS = [
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
]


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
  schema: str,
  columns: list[str],
  pickup: str,
  dropoff: str,
  files: str | list[str],
) -> None:

  stage_table = f"{table}_stage"
  conn.execute(
    f"""
    CREATE OR REPLACE TEMP TABLE {stage_table} (
      {schema}
    )
    """,
  )

  for file in files:
    conn.execute(
      f"""
      INSERT INTO {stage_table}
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
        {', '.join(columns)}
      FROM read_parquet(
        ?
      )
      """,
      [
        file,
        file
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

def ingest_data(
  *,
  files_path: str | list[str],
):
  conn = duckdb.connect(DUCKDB_PATH)

  table = TABLE_YELLOW_TRIPS
  columns = COLUMNS_YELLOW_TRIPS
  schema = SCHEMA_YELLOW_TRIPS
  pickup = 'tpep_pickup_datetime'
  dropoff = 'tpep_dropoff_datetime'

  try:
    create_table(
      conn,
      table=table,
      schema=schema,
    )

    stage_data(
      conn,
      table=table,
      columns=columns,
      schema=schema,
      pickup=pickup,
      dropoff=dropoff,
      files=files_path
    )

    merge_data(
      conn,
      table=table,
    )

  finally:
    conn.close()

