import os

import dotenv
from sqlalchemy import create_engine

dotenv.load_dotenv()

def db_connect():
  POSTGRES_USER = os.getenv("POSTGRES_USER")
  POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
  POSTGRES_DB = os.getenv("POSTGRES_DB")
  POSTGRES_HOST = os.getenv("POSTGRES_HOST")
  POSTGRES_PORT = os.getenv("POSTGRES_PORT")

  return create_engine(f'postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
