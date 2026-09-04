from pyspark.sql import SparkSession
import requests

spark = (
  SparkSession.builder
    .master("spark://spark-master:7077")
    .appName('test')
    .getOrCreate()
)

def download(url: str, file: str):
  response = requests.get(url)
  response.raise_for_status()

  with open(file, "wb") as f:
    f.write(response.content)

csv_path = "data/taxi_zone_lookup.csv"
download("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv", csv_path)

df = (
  spark.read
    .option('header', 'true')
    .csv(csv_path)
)
df.show()
df.write.parquet('data/zones')
