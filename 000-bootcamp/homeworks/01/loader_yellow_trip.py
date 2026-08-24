import pandas as pd
from sqlalchemy import Engine
from tqdm import tqdm

from common import create_table, load_csv_iterator
from config import db_connect
from config_yellow_trip import source, dtype, parse_dates, table_name


def load_yellow_trips():
  def process_chunk(engine: Engine, df_chunk: pd.DataFrame, table_name: str):
    df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
    print(" Inserted yellow trips:", len(df_chunk))

  df_iter = load_csv_iterator(source, dtype, parse_dates)

  engine = db_connect()

  create_table(engine, dtype, table_name, parse_dates)

  for df_chunk in tqdm(df_iter, desc="Reading"):
    process_chunk(engine, df_chunk, table_name)

