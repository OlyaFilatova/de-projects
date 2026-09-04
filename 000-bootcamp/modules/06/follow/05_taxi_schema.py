from typing import Literal
from pyspark.sql import SparkSession, types

spark = (
  SparkSession.builder
    .master("spark://spark-master:7077")
    .appName('taxi_schema')
    .getOrCreate()
)

green_schema = types.StructType([
    types.StructField("VendorID", types.IntegerType(), True),
    types.StructField("lpep_pickup_datetime", types.TimestampType(), True),
    types.StructField("lpep_dropoff_datetime", types.TimestampType(), True),
    types.StructField("store_and_fwd_flag", types.StringType(), True),
    types.StructField("RatecodeID", types.IntegerType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("passenger_count", types.IntegerType(), True),
    types.StructField("trip_distance", types.DoubleType(), True),
    types.StructField("fare_amount", types.DoubleType(), True),
    types.StructField("extra", types.DoubleType(), True),
    types.StructField("mta_tax", types.DoubleType(), True),
    types.StructField("tip_amount", types.DoubleType(), True),
    types.StructField("tolls_amount", types.DoubleType(), True),
    types.StructField("ehail_fee", types.DoubleType(), True),
    types.StructField("improvement_surcharge", types.DoubleType(), True),
    types.StructField("total_amount", types.DoubleType(), True),
    types.StructField("payment_type", types.IntegerType(), True),
    types.StructField("trip_type", types.IntegerType(), True),
    types.StructField("congestion_surcharge", types.DoubleType(), True)
])

yellow_schema = types.StructType([
    types.StructField("VendorID", types.IntegerType(), True),
    types.StructField("tpep_pickup_datetime", types.TimestampType(), True),
    types.StructField("tpep_dropoff_datetime", types.TimestampType(), True),
    types.StructField("passenger_count", types.IntegerType(), True),
    types.StructField("trip_distance", types.DoubleType(), True),
    types.StructField("RatecodeID", types.IntegerType(), True),
    types.StructField("store_and_fwd_flag", types.StringType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("payment_type", types.IntegerType(), True),
    types.StructField("fare_amount", types.DoubleType(), True),
    types.StructField("extra", types.DoubleType(), True),
    types.StructField("mta_tax", types.DoubleType(), True),
    types.StructField("tip_amount", types.DoubleType(), True),
    types.StructField("tolls_amount", types.DoubleType(), True),
    types.StructField("improvement_surcharge", types.DoubleType(), True),
    types.StructField("total_amount", types.DoubleType(), True),
    types.StructField("congestion_surcharge", types.DoubleType(), True)
])

sources: list[tuple[
  Literal['yellow'] | Literal['green'],
  list[tuple[int, range]]
]] = [('yellow', [(2020, range(1, 13)), (2021, range(1, 8))]), ('green', [(2020, range(1, 13)), (2021, range(1, 8))])]

def process_taxi_year(taxi: Literal['yellow'] | Literal['green'], year: int, months: range):
  schema = green_schema if taxi == 'green' else yellow_schema
  for month in months:
      print(f'processing data for {year}/{month}')

      input_path = f'data/raw/{taxi}/{year}/{month:02d}/'
      output_path = f'data/pq/{taxi}/{year}/{month:02d}/'
      
      df_green = spark.read \
          .option("header", "true") \
          .schema(schema) \
          .csv(input_path)

      df_green \
          .repartition(4) \
          .write.parquet(output_path, mode='overwrite')

[process_taxi_year(taxi, year, months)
  for taxi, years in sources
  for year, months in years]
