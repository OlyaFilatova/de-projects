from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook

from config.taxi import TAXI_CONFIG

def create_tables(
  hook: PostgresHook,
  *,
  table: str,
  staging_table: str,
  schema: str,
) -> None:
  hook.run(
    f"""
    CREATE TABLE IF NOT EXISTS {table} (
      {schema}
    );

    CREATE TABLE IF NOT EXISTS {staging_table} (
      {schema}
    );
    """
  )


def truncate_staging(
  hook: PostgresHook,
  staging_table: str,
) -> None:
  hook.run(
    f"TRUNCATE TABLE {staging_table}"
  )

def copy_to_staging(
  hook: PostgresHook,
  *,
  csv_file: Path,
  staging_table: str,
  columns: tuple[str, ...],
) -> None:

  conn = hook.get_conn()

  columns_sql = ", ".join(columns)

  copy_sql = f"""
    COPY {staging_table} ({columns_sql})
    FROM STDIN
    WITH (
      FORMAT CSV,
      HEADER TRUE
    )
  """

  with conn.cursor() as cursor:
    with cursor.copy(copy_sql) as copy:
      with csv_file.open("rb") as file:
        while data := file.read(1024 * 1024):
          copy.write(data)

  conn.commit()

def populate_metadata(
  hook: PostgresHook,
  *,
  staging_table: str,
  filename: str,
  datetime_columns: tuple[str, str],
) -> None:
  pickup, dropoff = datetime_columns

  hook.run(
    f"""
    UPDATE {staging_table}
    SET
      unique_row_id = md5(
        COALESCE(CAST(VendorID AS text), '') ||
        COALESCE(CAST({pickup} AS text), '') ||
        COALESCE(CAST({dropoff} AS text), '') ||
        COALESCE(PULocationID, '') ||
        COALESCE(DOLocationID, '') ||
        COALESCE(CAST(fare_amount AS text), '') ||
        COALESCE(CAST(trip_distance AS text), '')
      ),
      filename = %s
    """,
    parameters=(filename,),
  )

def merge_to_target(
  hook: PostgresHook,
  *,
  table: str,
  staging_table: str,
  columns: tuple[str, ...],
) -> None:

  insert_columns = (
    "unique_row_id",
    "filename",
    *columns,
  )

  column_list = ", ".join(insert_columns)

  source_columns = ", ".join(
    f"S.{column}"
    for column in insert_columns
  )

  hook.run(
    f"""
    MERGE INTO {table} AS T
    USING {staging_table} AS S
    ON T.unique_row_id = S.unique_row_id

    WHEN NOT MATCHED THEN
      INSERT ({column_list})
      VALUES ({source_columns});
    """
  )

def load_taxi_data(
  csv_file: str | Path,
  *,
  taxi: str,
  filename: str,
  postgres_conn_id: str,
) -> None:

  csv_file = Path(csv_file)

  config = TAXI_CONFIG[taxi]

  table = f"public.{taxi}_tripdata"
  staging_table = f"public.{taxi}_tripdata_staging"

  hook = PostgresHook(
    postgres_conn_id=postgres_conn_id,
  )

  create_tables(
    hook,
    table=table,
    staging_table=staging_table,
    schema=config.schema,
  )

  truncate_staging(
    hook,
    staging_table,
  )

  copy_to_staging(
    hook,
    csv_file=csv_file,
    staging_table=staging_table,
    columns=config.columns,
  )

  populate_metadata(
    hook,
    staging_table=staging_table,
    filename=filename,
    datetime_columns=config.datetime_columns,
  )

  merge_to_target(
    hook,
    table=table,
    staging_table=staging_table,
    columns=config.columns,
  )
