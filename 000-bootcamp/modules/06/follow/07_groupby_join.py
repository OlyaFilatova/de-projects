from typing import Literal

from pyspark.sql import SparkSession

spark = (
  SparkSession.builder
    .master("spark://spark-master:7077")
    .appName('groupby_join')
    .getOrCreate()
)

def create_revenue_table(taxi: Literal['yellow'] | Literal['green']):
  pickup_column = 'lpep_pickup_datetime' if taxi == 'green' else 'tpep_pickup_datetime'
  df = spark.read.parquet(f'data/pq/{taxi}/*/*')

  df.registerTempTable(taxi)

  df_revenue = spark.sql(f"""
  SELECT
    date_trunc('hour', {pickup_column}) as hour,
    PULocationID as zone,
    SUM(total_amount) as amount,
    COUNT(1) as number_records
  FROM {taxi}
  WHERE
    {pickup_column} >= '2020-01-01 00:00:00'
  GROUP BY
    1, 2
  """)

  df_revenue \
    .repartition(20) \
    .write.parquet(f'data/report/revenue/{taxi}', mode='overwrite')
  return df_revenue

create_revenue_table('green')
create_revenue_table('yellow')

df_green_revenue = spark.read.parquet('data/report/revenue/green')
df_yellow_revenue = spark.read.parquet('data/report/revenue/yellow')

df_green_revenue_tmp = df_green_revenue \
  .withColumnRenamed('amount', 'green_amount') \
  .withColumnRenamed('number_records', 'green_number_records')

df_yellow_revenue_tmp = df_yellow_revenue \
  .withColumnRenamed('amount', 'yellow_amount') \
  .withColumnRenamed('number_records', 'yellow_number_records')

df_join = df_green_revenue_tmp.join(df_yellow_revenue_tmp, on=['hour', 'zone'], how='outer')
df_join.write.parquet('data/report/revenue/total', mode='overwrite')

df_join = spark.read.parquet('data/report/revenue/total')

print(df_join)

df_zones = spark.read.parquet('data/zones/')

df_result = df_join.join(df_zones, df_join.zone == df_zones.LocationID)
df_result.drop('LocationID', 'zone').write.parquet('data/revenue-zones')
