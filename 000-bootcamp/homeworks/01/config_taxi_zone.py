from typing import Hashable, Mapping

from pandas._typing import Dtype

dtype: Mapping[Hashable, Dtype] = {
  "LocationID": "Int64",
  "Borough": "string",
  "Zone": "string",
  "service_zone": "string"
}

parse_dates = []

source = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
table_name = 'taxi_zone_lookup'
