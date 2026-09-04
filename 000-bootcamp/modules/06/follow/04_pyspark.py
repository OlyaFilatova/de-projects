import gzip
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import requests
import shutil

spark = (
  SparkSession.builder
    .master("spark://spark-master:7077")
    .appName('pyspark')
    .getOrCreate()
)

def download(url: str, file: str):
  response = requests.get(url)
  response.raise_for_status()

  with open(file, "wb") as f:
    f.write(response.content)

csv_gz_path = "data/fhvhv_tripdata_2021-01.csv.gz"
download("https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz", csv_gz_path)

def unzip(source: str, target: str):
  with gzip.open(source, "rb") as f_in:
    with open(target, "wb") as f_out:
      shutil.copyfileobj(f_in, f_out)

csv_path = "data/fhvhv_tripdata_2021-01.csv"

unzip(csv_gz_path, csv_path)

schema = types.StructType([
  types.StructField('hfhs_license_num', types.StringType(), True),
  types.StructField('dispatching_base_num', types.StringType(), True),
  types.StructField('pickup_datetime', types.TimestampType(), True),
  types.StructField('dropoff_datetime', types.TimestampType(), True),
  types.StructField('PULocationID', types.IntegerType(), True),
  types.StructField('DOLocationID', types.IntegerType(), True),
  types.StructField('SR_Flag', types.StringType(), True),
])

df = (
  spark.read
    .option('header', 'true')
    .schema(schema)
    .csv(csv_path)
)

df = df.repartition(24)

parquet_path = 'data/fhvhv/2021/01'
df.write.mode("overwrite").parquet(parquet_path)

df = spark.read.parquet(parquet_path)
df.printSchema()
df.show()

def crazy_stuff(base_num):
  num = int(base_num[1:])
  if num % 7 == 0:
    return f's/{num:03x}'
  elif num % 3 == 0:
    return f'a/{num:03x}'
  else:
    return f'e/{num:03x}'

print(crazy_stuff('B02884'))

crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())

(
df.withColumn('pickup_date', F.to_date(df.pickup_datetime))
  .withColumn('dropoff_date', F.to_date(df.dropoff_datetime))
  .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num))
  .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID')
  .show()
)

(
df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID')
  .filter(df.hfhs_license_num == 'HV0003')
  .show()
)