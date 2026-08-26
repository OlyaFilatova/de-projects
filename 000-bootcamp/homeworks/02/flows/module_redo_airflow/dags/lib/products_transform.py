import pandas as pd

def transform_products(
    data: dict,
    columns_to_keep: list[str] = ["brand", "price"],
  ):
  return pd.DataFrame([
    {
      column: product.get(column, "N/A")
      for column in columns_to_keep
    }
    for product in data["products"]
  ])
