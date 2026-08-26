from dataclasses import dataclass

DATA_URL = (
  "https://github.com/DataTalksClub/nyc-tlc-data"
  "/releases/download/{taxi}/{file}.gz"
)

@dataclass(frozen=True)
class TaxiConfig:
  datetime_columns: tuple[str, str]
  columns: tuple[str, ...]
  schema: str

@dataclass(frozen=True)
class TaxiDuckDBConfig:
  table: str
  pickup: str
  dropoff: str
  schema: str

TAXI_CONFIG = {
  "yellow": TaxiConfig(
    datetime_columns=(
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
    ),
    columns=(
      "VendorID",
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "passenger_count",
      "trip_distance",
      "RatecodeID",
      "store_and_fwd_flag",
      "PULocationID",
      "DOLocationID",
      "payment_type",
      "fare_amount",
      "extra",
      "mta_tax",
      "tip_amount",
      "tolls_amount",
      "improvement_surcharge",
      "total_amount",
      "congestion_surcharge",
    ),
    schema="""
      unique_row_id text,
      filename text,
      VendorID text,
      tpep_pickup_datetime timestamp,
      tpep_dropoff_datetime timestamp,
      passenger_count integer,
      trip_distance double precision,
      RatecodeID text,
      store_and_fwd_flag text,
      PULocationID text,
      DOLocationID text,
      payment_type integer,
      fare_amount double precision,
      extra double precision,
      mta_tax double precision,
      tip_amount double precision,
      tolls_amount double precision,
      improvement_surcharge double precision,
      total_amount double precision,
      congestion_surcharge double precision
    """,
  ),

  "green": TaxiConfig(
    datetime_columns=(
      "lpep_pickup_datetime",
      "lpep_dropoff_datetime",
    ),
    columns=(
      "VendorID",
      "lpep_pickup_datetime",
      "lpep_dropoff_datetime",
      "store_and_fwd_flag",
      "RatecodeID",
      "PULocationID",
      "DOLocationID",
      "passenger_count",
      "trip_distance",
      "fare_amount",
      "extra",
      "mta_tax",
      "tip_amount",
      "tolls_amount",
      "ehail_fee",
      "improvement_surcharge",
      "total_amount",
      "payment_type",
      "trip_type",
      "congestion_surcharge",
    ),
    schema="""
      unique_row_id text,
      filename text,
      VendorID text,
      lpep_pickup_datetime timestamp,
      lpep_dropoff_datetime timestamp,
      store_and_fwd_flag text,
      RatecodeID text,
      PULocationID text,
      DOLocationID text,
      passenger_count integer,
      trip_distance double precision,
      fare_amount double precision,
      extra double precision,
      mta_tax double precision,
      tip_amount double precision,
      tolls_amount double precision,
      ehail_fee double precision,
      improvement_surcharge double precision,
      total_amount double precision,
      payment_type integer,
      trip_type integer,
      congestion_surcharge double precision
    """,
  ),
}

TAXI_DUCKDB_CONFIG = {
    "yellow": TaxiDuckDBConfig(
        table="yellow_tripdata",
        pickup="tpep_pickup_datetime",
        dropoff="tpep_dropoff_datetime",
        schema="""
            unique_row_id VARCHAR,
            filename VARCHAR,
            VendorID VARCHAR,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DECIMAL(18,3),
            RatecodeID VARCHAR,
            store_and_fwd_flag VARCHAR,
            PULocationID VARCHAR,
            DOLocationID VARCHAR,
            payment_type INTEGER,
            fare_amount DECIMAL(18,3),
            extra DECIMAL(18,3),
            mta_tax DECIMAL(18,3),
            tip_amount DECIMAL(18,3),
            tolls_amount DECIMAL(18,3),
            improvement_surcharge DECIMAL(18,3),
            total_amount DECIMAL(18,3),
            congestion_surcharge DECIMAL(18,3)
        """,
    ),
    "green": TaxiDuckDBConfig(
        table="green_tripdata",
        pickup="lpep_pickup_datetime",
        dropoff="lpep_dropoff_datetime",
        schema="""
            unique_row_id VARCHAR,
            filename VARCHAR,
            VendorID VARCHAR,
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            store_and_fwd_flag VARCHAR,
            RatecodeID VARCHAR,
            PULocationID VARCHAR,
            DOLocationID VARCHAR,
            passenger_count INTEGER,
            trip_distance DECIMAL(18,3),
            fare_amount DECIMAL(18,3),
            extra DECIMAL(18,3),
            mta_tax DECIMAL(18,3),
            tip_amount DECIMAL(18,3),
            tolls_amount DECIMAL(18,3),
            ehail_fee DECIMAL(18,3),
            improvement_surcharge DECIMAL(18,3),
            total_amount DECIMAL(18,3),
            payment_type INTEGER,
            trip_type INTEGER,
            congestion_surcharge DECIMAL(18,3)
        """,
    ),
}
