from pyspark.sql import SparkSession

spark = (
  SparkSession.builder
    .master("spark://spark-master:7077")
    .appName('test')
    .getOrCreate()
)

print('=================version start=================')
print('version', spark.version)
print('=================version end=================')
