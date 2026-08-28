# 3. How many rows are there 
# for the `Yellow` Taxi data for all CSV files in the year 2020?
from airflow.sdk import dag, task

from config.taxi import DATA_URL

from lib.download_taxi_csv import download_taxi_data
from lib.taxi_duckdb import row_count


@dag(
  dag_id="15_homework_3",
  schedule=None,
  catchup=False,
  tags=["nyc-taxi", "postgres", "etl"],
)
def homework_3():
  @task
  def build_config(params=None) -> dict:
    taxi = 'yellow'
    year = '2020'

    files = [{
      "taxi": taxi,
      "filename": f"{taxi}_tripdata_{year}-{i:02d}.csv"
    } for i in range(1, 13)]

    return {
      "taxi": taxi,
      "year": year,
      "files": files,
    }

  @task
  def count_rows(config: dict) -> int:
    csv_files = [str(download_taxi_data(
      DATA_URL,
      taxi=file["taxi"],
      filename=file["filename"],
    )) for file in config["files"]]

    count = row_count(
      files_path=csv_files
    )

    print(f"There are {count} rows the `Yellow` Taxi data for all CSV files in the year 2020.")

    return count

  config = build_config()
  count_rows(config)

homework_3()
