from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
  SparkSession.builder
    .master("spark://spark-master:7077")
    .appName('spark_sql')
    .getOrCreate()
)

df_green = spark.read.parquet('data/pq/green/*/*')

df_green = df_green \
  .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
  .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.parquet('data/pq/yellow/*/*')

df_yellow = df_yellow \
  .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
  .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

yellow_columns = set(df_yellow.columns)
green_columns = set(df_green.columns)

common_columns = [*yellow_columns.intersection(green_columns)]

df_green_sel = df_green \
  .select(common_columns) \
  .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
  .select(common_columns) \
  .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.groupBy('service_type').count().show()

print('=======================columns=======================')
print(df_green.columns)
print(df_yellow.columns)
print(df_trips_data.columns)

df_trips_data.registerTempTable('trips_data')

spark.sql("""
SELECT service_type, count(1)
FROM trips_data
GROUP BY service_type
""").show()

df_result = spark.sql("""
SELECT
  PULocationID as revenue_zone,
  date_trunc('month', pickup_datetime) as revenue_month,
  service_type,

  SUM(fare_amount) AS revenue_monthly_fare,
  SUM(extra) AS revenue_monthly_extra,
  SUM(mta_tax) AS revenue_monthly_mta_tax,
  SUM(tip_amount) AS revenue_monthly_tip_amount,
  SUM(tolls_amount) AS revenue_monthly_tolls_amount,
  SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
  SUM(total_amount) AS revenue_monthly_total_amount,
  SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

  AVG(passenger_count) AS avg_monthly_passenger_count,
  AVG(trip_distance) AS avg_monthly_trip_distance
FROM trips_data
GROUP BY
  1, 2, 3
""")

df_result.coalesce(1).write.parquet('data/report/revenue/', mode='overwrite')
