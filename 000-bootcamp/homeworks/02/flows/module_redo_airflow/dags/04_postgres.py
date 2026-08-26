from datetime import datetime

from airflow.sdk import dag, task

from config.params import taxi as taxi_param, year as year_param, month as month_param
from config.postgres import POSTGRES_CONN_ID
from config.taxi import DATA_URL

from lib.download_taxi_csv import download_taxi_data
from lib.taxi_postgres import load_taxi_data


@dag(
  dag_id="04_postgres",
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  tags=["nyc-taxi", "postgres", "etl"],
  params={
    "taxi": taxi_param,
    "year": year_param,
    "month": month_param,
  },
)
def nyc_taxi_to_postgres():
  @task
  def build_config(params=None) -> dict:
    taxi = params["taxi"]
    year = params["year"]
    month = params["month"]

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
  def download(config: dict) -> dict:
    csv_file = download_taxi_data(
      DATA_URL,
      taxi=config["taxi"],
      filename=config["filename"],
    )

    return {
      **config,
      "local_file": str(csv_file),
    }

  @task
  def load(config: dict) -> None:
    load_taxi_data(
      csv_file=config["local_file"],
      taxi=config["taxi"],
      filename=config["filename"],
      postgres_conn_id=POSTGRES_CONN_ID,
    )

  config = build_config()
  downloaded = download(config)
  load(downloaded)

nyc_taxi_to_postgres()
