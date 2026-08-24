import pandas as pd
from sqlalchemy import Engine
from tqdm import tqdm

from common import create_table, load_parquet_iterator, load_remote_file
from config import db_connect
from config_green_trip import source, dtype, parse_dates, table_name


def load_green_trips():
  def process_chunk(engine: Engine, df_chunk: pd.DataFrame, table_name: str):
    df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
    print(" Inserted green trips:", len(df_chunk))

  local_source = '.data/green_trips.parquet'
  load_remote_file(source, local_source)

  df_iter = load_parquet_iterator(local_source)

  engine = db_connect()

  create_table(engine, dtype, table_name, parse_dates)

  for df_chunk in tqdm(df_iter, desc="Reading"):
    process_chunk(engine, df_chunk, table_name)

