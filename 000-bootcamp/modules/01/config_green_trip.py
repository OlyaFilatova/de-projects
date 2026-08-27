from typing import Hashable, Mapping

from pandas._typing import Dtype

dtype: Mapping[Hashable, Dtype] = {
  "VendorID": "Int64",
  "passenger_count": "Int64",
  "trip_distance": "float64",
  "RatecodeID": "Int64",
  "store_and_fwd_flag": "string",
  "PULocationID": "Int64",
  "DOLocationID": "Int64",
  "payment_type": "Int64",
  "fare_amount": "float64",
  "extra": "float64",
  "mta_tax": "float64",
  "tip_amount": "float64",
  "tolls_amount": "float64",
  "improvement_surcharge": "float64",
  "total_amount": "float64",
  "congestion_surcharge": "float64",
  "lpep_pickup_datetime": "object",
  "lpep_dropoff_datetime": "object",
  "ehail_fee": "float64",
  "trip_type": "Int64",
  "cbd_congestion_fee": "float64"
}

parse_dates = [
  "lpep_pickup_datetime",
  "lpep_dropoff_datetime"
]


source = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
table_name = 'green_trip_data'
