from typing import Hashable, Mapping

import pandas as pd
from pandas._typing import Dtype
from sqlalchemy import Engine

def load_csv_iterator(source: str, dtype: Mapping[Hashable, Dtype], parse_dates: list[str]):
  return pd.read_csv(
    source,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=60_000)

def create_table(engine: Engine, dtypes: Mapping[Hashable, Dtype], table_name: str, parse_dates: list[str]):
  df = pd.DataFrame({
    column: pd.Series(dtype="datetime64[ns]" if column in parse_dates else dtype)
    for column, dtype in dtypes.items()
  })
  df.to_sql(
    name=table_name,
    con=engine,
    if_exists="replace"
  )
  print(f"Created `{table_name}` table")
