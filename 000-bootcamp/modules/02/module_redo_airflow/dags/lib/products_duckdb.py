import duckdb
from pandas import DataFrame

def select_products(
  df: DataFrame,
  sql_create_products_table: str,
  sql_select_products: str
):
  connection = duckdb.connect()

  connection.execute(sql_create_products_table)

  result = connection.execute(sql_select_products).fetchall()

  connection.close()

  return result
