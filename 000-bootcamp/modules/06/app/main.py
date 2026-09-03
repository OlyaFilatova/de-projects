from pyspark.sql import SparkSession

spark = (
  SparkSession.builder
  .appName("DockerPySpark")
  .master("spark://localhost:7077")
  # .master("local[*]")
  .getOrCreate()
)

data = [
  ("Alice", 25),
  ("Bob", 30),
  ("Charlie", 35)
]

df = spark.createDataFrame(data, ["name", "age"])

df.show()
df.groupBy().avg("age").show()

spark.stop()
