from sqlalchemy import text

from common import terminal_bold
from config_green_trip import table_name
from loader_green_trip import load_green_trips
from config import db_connect

start_date_inclusive = '2025-11-01'
end_date_exclusive = '2025-12-01'
trip_distance_max_inclusive = 1

if __name__ == '__main__':
  load_green_trips()

  engine = db_connect()
  with engine.connect() as connection:
    count = connection.execute(
      text(f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE 
          trip_distance <= :trip_distance_max_inclusive AND
          lpep_pickup_datetime >= :start_date_inclusive AND
          lpep_pickup_datetime < :end_date_exclusive
      """),
      {
        "trip_distance_max_inclusive": trip_distance_max_inclusive,
        "start_date_inclusive": start_date_inclusive,
        "end_date_exclusive": end_date_exclusive
      }
    ).scalar()

  if count is None:
    print("Error: Failed to count trips in November 2025.")
  else:
    print(f"For the trips in November 2025, {terminal_bold(count)} trips had a trip distance of less than or equal to 1 mile.")
