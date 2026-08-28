# 1. Within the execution 
# for `Yellow` Taxi data for the year `2020` and month `12`: 
# what is the uncompressed file size 
# (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?

from pathlib import Path

from airflow.sdk import dag, task
from pendulum import datetime

from config.taxi import DATA_URL

from lib.download_and_unzip import download_and_unzip


@dag(
  dag_id="12_homework_1",
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  tags=["nyc-taxi", "s3", "duckdb", "etl"],
)
def homework_1():
  @task
  def build_config() -> dict:
    taxi = 'yellow'
    year = '2020'
    month = '12'
    filename = f"{taxi}_tripdata_{year}-{month}.csv"
    url = DATA_URL.format(
      taxi=taxi,
      file=filename,
    )
    return {
      "filename": filename,
      "url": url,
      "taxi": taxi,
      "year": year,
      "month": month
    }

  @task
  def download(
    config: dict,
  ) -> int:
    local_path = Path("/tmp") / config["filename"]

    download_and_unzip(config["url"], local_path)

    size = local_path.stat().st_size

    print(f"The uncompressed file size for `Yellow` Taxi data for the year `2020` and month `12` is: {size / (1024 * 1024)} MiB")

    return size

  config = build_config()
  download(config)

homework_1()
