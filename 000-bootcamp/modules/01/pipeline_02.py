from sqlalchemy import text

from common import terminal_bold
from config_green_trip import table_name
from loader_green_trip import load_green_trips
from config import db_connect

trip_distance_ceiling = 100

if __name__ == '__main__':
  load_green_trips()

  engine = db_connect()
  with engine.connect() as connection:
    data = connection.execute(
      text(f"""
        SELECT DATE(lpep_pickup_datetime) as day, trip_distance
        FROM {table_name}
        WHERE 
          trip_distance < :trip_distance_ceiling
        ORDER BY trip_distance DESC
        LIMIT 1
      """),
      {
        "trip_distance_ceiling": trip_distance_ceiling
      }
    ).first()

  if not data:
    print("Error: Failed to find pick up day with the longest trip.")
  else:
    print(f"{terminal_bold(data.day)} was the pick up day with the longest trip distance: {terminal_bold(data.trip_distance)}. (excluding trips with distance of 100 miles and more.)")
