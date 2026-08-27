from datetime import datetime

from airflow.sdk import dag, get_current_context, task, Param

from config.taxi import DATA_URL
from config.postgres import POSTGRES_CONN_ID

from lib.download_taxi_csv import download_taxi_data
from lib.taxi_postgres import load_taxi_data


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
      )
    },
  )
  def nyc_taxi_to_postgres():
    @task
    def build_config(params=None) -> dict:
      taxi = params["taxi"]

      context = get_current_context()
      date = context["logical_date"].strftime("%Y-%m")

      filename = f"{taxi}_tripdata_{date}.csv"

      filename = (
        f"{taxi}_tripdata_{date}.csv"
      )

      return {
        "taxi": taxi,
        "date": date,
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
