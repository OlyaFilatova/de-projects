from sqlalchemy import text

from common import terminal_bold
from config_green_trip import table_name as green_trip_table_name
from config_taxi_zone import table_name as taxi_zone_table_name
from loader_green_trip import load_green_trips
from config import db_connect
from loader_taxi_zone import load_taxi_zones

zone_name = "East Harlem North"
start_date_inclusive = '2025-11-01'
end_date_exclusive = '2025-12-01'


if __name__ == '__main__':
  load_green_trips()
  load_taxi_zones()

  engine = db_connect()
  with engine.connect() as connection:
    data = connection.execute(
      text(f"""SELECT largest_tip_trip.tip_amount as tip, largest_tip_trip."DOLocationID" as zone_id, zone."Zone" as zone
        FROM (SELECT trips.tip_amount, trips."DOLocationID"
        FROM {green_trip_table_name} as trips
        WHERE trips."PULocationID" = (SELECT "LocationID" FROM {taxi_zone_table_name} WHERE "Zone" = :zone_name LIMIT 1) AND
          lpep_pickup_datetime >= :start_date_inclusive AND
          lpep_pickup_datetime < :end_date_exclusive
          ORDER BY trips.tip_amount DESC
          LIMIT 1) as largest_tip_trip
        LEFT JOIN {taxi_zone_table_name} as zone
        ON largest_tip_trip."DOLocationID" = zone."LocationID"
      """),
      {
        "zone_name": zone_name,
        "start_date_inclusive": start_date_inclusive,
        "end_date_exclusive": end_date_exclusive,
      }
    ).first()

  if not data:
    print("Error: Failed to find pick up zone with the largest tip.")
  else:
    print(f"In November, for trips with pick up zone named \"East Harlem North\" {terminal_bold(data.zone)}({terminal_bold(data.zone_id)}) was the drop off zone with the largest tip: {terminal_bold(data.tip)}.")
