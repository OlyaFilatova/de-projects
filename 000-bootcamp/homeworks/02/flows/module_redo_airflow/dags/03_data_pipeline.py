from datetime import datetime

from airflow.sdk import dag, task, Param
from pandas import DataFrame


@dag(
  dag_id="03_data_pipeline_1",
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  params={
    "columns_to_keep": Param(
      type="array",
      items={"type": "string"},
      default=['brand', 'price']
    )
  }
)
def data_pipeline():

  @task
  def extract():
    import requests

    response = requests.get(
      "https://dummyjson.com/products",
      timeout=30,
    )
    response.raise_for_status()

    return response.json()

  @task
  def get_columns_to_keep(**kwargs) -> list[str]:
    return kwargs["params"]["columns_to_keep"]

  @task.virtualenv(
    system_site_packages=False,
    requirements=["pandas"],
  )
  def transform(
    data: dict,
    columns_to_keep: list[str] = ["brand", "price"],
  ):
    import pandas as pd

    return pd.DataFrame([
      {
        column: product.get(column, "N/A")
        for column in columns_to_keep
      }
      for product in data["products"]
    ])

  @task.virtualenv(
    system_site_packages=False,
    requirements=["duckdb", "pandas", "pyarrow"],
  )
  def query(df: DataFrame):
    import duckdb

    connection = duckdb.connect()

    connection.execute(
      """
      CREATE TABLE products AS
      SELECT * FROM df
      """
    )

    result = connection.execute(
      """
      SELECT
        brand,
        ROUND(AVG(price), 2) AS avg_price
      FROM products
      GROUP BY brand
      ORDER BY avg_price DESC
      """
    ).fetchall()

    connection.close()

    return result

  data = extract()
  columns = get_columns_to_keep()
  products = transform(data, columns)
  query(products)


data_pipeline()
