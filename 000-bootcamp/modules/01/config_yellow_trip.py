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
  "tpep_pickup_datetime": "object",
  "tpep_dropoff_datetime": "object"
}

parse_dates = [
  "tpep_pickup_datetime",
  "tpep_dropoff_datetime"
]

source = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
table_name = 'yellow_taxi_data'
