from pathlib import Path

from airflow.sdk import dag, task
from pendulum import datetime

from lib.download_and_unzip import download_and_unzip
from lib.upload_s3 import upload as upload_s3

from lib.taxi_duckdb import load_from_s3
from config.params import taxi as taxi_param, year as year_param, month as month_param
from config.taxi import DATA_URL


BUCKET_NAME = "bucket1"
AWS_CONN_ID = "localstack"

@dag(
  dag_id="08_s3_duckdb_pipeline",
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  tags=["nyc-taxi", "s3", "duckdb", "etl"],
  params={
    "taxi": taxi_param,
    "year": year_param,
    "month": month_param,
  },
)
def s3_duckdb_pipeline():
  @task
  def build_filename(params=None) -> str:
    return (
      f"{params['taxi']}_tripdata_"
      f"{params['year']}-{params['month']}.csv"
    )

  @task
  def upload(
    filename: str,
    params=None,
  ) -> str:
    url = DATA_URL.format(
      taxi=params["taxi"],
      file=filename,
    )

    local_path = Path("/tmp") / filename

    download_and_unzip(url, local_path)

    upload_s3(filename, local_path, AWS_CONN_ID, BUCKET_NAME)

    return f"s3://{BUCKET_NAME}/{filename}"


  @task
  def load(
    s3_path: str,
    filename: str,
    params=None,
  ):
    load_from_s3(
      s3_path=s3_path,
      taxi=params["taxi"],
      filename=filename,
    )

  filename = build_filename()

  s3_path = upload(filename)

  load(
    s3_path,
    filename,
  )

s3_duckdb_pipeline()
