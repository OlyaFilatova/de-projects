from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task, Param
from pandas import DataFrame

from lib.load_json import load_json

DAG_DIR = Path(__file__).resolve().parent
sql_create_products_table_path = DAG_DIR / "include/sql/03_01_create_products.sql"
sql_select_products_path =  DAG_DIR / "include/sql/03_02_select_products.sql"


@dag(
  dag_id="03_data_pipeline",
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
    return load_json("https://dummyjson.com/products")

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
    from lib.products_transform import transform_products

    return transform_products(data, columns_to_keep)

  @task.virtualenv(
    system_site_packages=False,
    requirements=["duckdb", "pandas", "pyarrow"],
  )
  def query(
    df: DataFrame,
    sql_create_products_table_path: Path,
    sql_select_products_path: Path
  ):
    from lib.products_duckdb import select_products

    sql_create_products_table = sql_create_products_table_path.read_text()
    sql_select_products = sql_select_products_path.read_text()

    return select_products(
      df,
      sql_create_products_table,
      sql_select_products
    )

  data = extract()
  columns = get_columns_to_keep()
  products = transform(data, columns)
  query(
    products,
    sql_create_products_table_path,
    sql_select_products_path
  )

data_pipeline()
