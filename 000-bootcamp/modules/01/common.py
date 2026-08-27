from typing import Any, Generator, Hashable, Mapping
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from pandas._typing import Dtype
import requests
from sqlalchemy import Engine
import pyarrow.parquet as pq

def load_remote_file(source: str, target: str):
  with requests.get(source, stream=True) as response:
    response.raise_for_status()

    with Path(target).open("wb") as f:
      for chunk in response.iter_content(chunk_size=1024 * 1024):
        if chunk:
          f.write(chunk)

def load_csv_iterator(source: str, dtype: Mapping[Hashable, Dtype], parse_dates: list[str]):
  return pd.read_csv(
    source,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=60_000)

def load_parquet_iterator(source: str) -> Generator[pd.DataFrame, Any, None]:
  parquet_file = pq.ParquetFile(source)
  for batch in parquet_file.iter_batches(
    batch_size=60_000,
    use_pandas_metadata=True
  ):
    yield batch.to_pandas()

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

def terminal_bold(text: str) -> str:
  return f"\033[1m{text}\033[0m"
