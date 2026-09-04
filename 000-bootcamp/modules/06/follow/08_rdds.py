from collections import namedtuple
from datetime import datetime

from pyspark.sql import Row, SparkSession, types

spark = (
  SparkSession.builder
    .master("spark://spark-master:7077")
    .appName('groupby_join')
    .getOrCreate()
)

df_green = spark.read.parquet('data/pq/green/*/*')
rdd = df_green \
  .select('lpep_pickup_datetime', 'PULocationID', 'total_amount') \
  .rdd

start = datetime(year=2020, month=1, day=1)

def filter_outliers(row: Row):
  return row.lpep_pickup_datetime >= start

rows = rdd.take(10)
row = rows[0]

print(row)

def prepare_for_grouping(row: Row):
  hour = row.lpep_pickup_datetime.replace(minute=0, second=0, microsecond=0)
  zone = row.PULocationID
  key = (hour, zone)

  amount = row.total_amount
  count = 1
  value = (amount, count)

  return (key, value)

def calculate_revenue(left_value, right_value):
  left_amount, left_count = left_value
  right_amount, right_count = right_value

  output_amount = left_amount + right_amount
  output_count = left_count + right_count

  return (output_amount, output_count)

RevenueRow = namedtuple('RevenueRow', ['hour', 'zone', 'revenue', 'count'])

def unwrap(row):
  return RevenueRow(
    hour = row[0][0],
    zone = row[0][1],
    revenue = row[1][0],
    count = row[1][1]
  )

result_schema = types.StructType([
  types.StructField('hour', types.TimestampType(), True),
  types.StructField('zone', types.IntegerType(), True),
  types.StructField('revenue', types.DoubleType(), True),
  types.StructField('count', types.IntegerType(), True),
])

df_result = rdd \
  .filter(filter_outliers) \
  .map(prepare_for_grouping) \
  .reduceByKey(calculate_revenue) \
  .map(unwrap) \
  .toDF(result_schema)

df_result.write.parquet('data/green-revenue', mode='overwrite')

columns = ['VendorId', 'lpep_pickup_datetime', 'PULocationID', 'DOLocationID', 'trip_distance']

duration_rdd = df_green \
  .select(columns) \
  .rdd

rows = duration_rdd.take(10)

print(rows)

print(columns)
