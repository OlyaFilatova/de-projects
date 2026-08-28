# 5. How many rows are there for
# the `Yellow` Taxi data for the March 2021 CSV file?
from airflow.sdk import dag, task

from config.taxi import DATA_URL

from lib.download_taxi_csv import download_taxi_data
from lib.taxi_duckdb import row_count


@dag(
  dag_id="16_homework_5",
  schedule=None,
  catchup=False,
  tags=["nyc-taxi", "postgres", "etl"],
)
def homework_5():
  @task
  def build_config(params=None) -> dict:
    taxi = 'yellow'
    year = '2021'
    month = '03'

    filename = (
      f"{taxi}_tripdata_{year}-{month}.csv"
    )

    return {
      "taxi": taxi,
      "year": year,
      "month": month,
      "filename": filename,
    }

  @task
  def count_rows(config: dict) -> int:
    csv_file = download_taxi_data(
      DATA_URL,
      taxi=config["taxi"],
      filename=config["filename"],
    )

    count = row_count(
      files_path=str(csv_file)
    )

    print(f"There are {count} rows for the `Yellow` Taxi data for the March 2021 CSV file.")

    return count

  config = build_config()
  count_rows(config)

homework_5()
