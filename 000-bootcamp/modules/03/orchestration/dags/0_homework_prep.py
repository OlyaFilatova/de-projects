# 1. What is count of records 
# for the 2024 Yellow Taxi Data?
from airflow.sdk import dag, task

from lib.download import download_file
from lib.taxi_duckdb import ingest_data

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

@dag(
  dag_id="0_homework_prep",
  schedule=None,
  catchup=False,
  tags=["nyc-taxi", "postgres", "etl"],
)
def homework_prep():
  @task
  def build_config(params=None) -> dict:
    taxi = 'yellow'
    year = '2024'

    files = [{
      "taxi": taxi,
      "filename": f"{taxi}_tripdata_{year}-{i:02d}.parquet"
    } for i in range(1, 7)]

    return {
      "taxi": taxi,
      "year": year,
      "files": files,
    }

  @task
  def create_table(config: dict) -> int:
    parquet_files = [str(download_file(
      URL_PREFIX + file["filename"],
      folder_prefix="nyc_taxi_",
      filename=file["filename"],
    )) for file in config["files"]]
    
    ingest_data(files_path=parquet_files)

  config = build_config()
  create_table(config)

homework_prep()
