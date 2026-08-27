from sqlalchemy import text

from common import terminal_bold
from config_green_trip import table_name as green_trip_table_name
from config_taxi_zone import table_name as taxi_zone_table_name
from loader_green_trip import load_green_trips
from config import db_connect
from loader_taxi_zone import load_taxi_zones

date = '2025-11-18' # November 18th, 2025

if __name__ == '__main__':
  load_green_trips()
  load_taxi_zones()

  engine = db_connect()
  with engine.connect() as connection:
    data = connection.execute(
      text(f"""SELECT popular_zone.c as trip_count, popular_zone."PULocationID" as zone_id, zone."Zone" as zone
        FROM (
          SELECT COUNT(*) as c, "PULocationID" 
          FROM {green_trip_table_name}
          WHERE DATE(lpep_pickup_datetime) = :date OR DATE(lpep_dropoff_datetime) = :date
          GROUP BY "PULocationID"
          ORDER BY c DESC
          LIMIT 1
        ) as popular_zone
        LEFT JOIN {taxi_zone_table_name} as zone
        ON
          popular_zone."PULocationID" = zone."LocationID"
      """),
      {
        "date": date
      }
    ).first()

  if not data:
    print("Error: Failed to find pick up zone with the largest sum of all trips.")
  else:
    print(f"On November 18th, 2025 {terminal_bold(data.zone)}({terminal_bold(data.zone_id)}) was the pick up zone with the largest sum of all trips: {terminal_bold(data.trip_count)}.")
