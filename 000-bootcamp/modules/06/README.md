# Module 6 

## Follow

```sh
docker compose up
```

```sh
docker compose run --rm pyspark \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/follow/03_test.py
```

```sh
docker compose run --rm pyspark \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/follow/04_pyspark.py
```

```sh
sh ./download_data.sh yellow 2020
sh ./download_data.sh yellow 2021
sh ./download_data.sh green 2020
sh ./download_data.sh green 2021
```

```sh
docker compose run --rm pyspark \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/follow/05_taxi_schema.py
```

```sh
docker compose run --rm pyspark \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/follow/06_spark_sql.py
```

```sh
docker compose run --rm pyspark \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/follow/07_groupby_join.py
```

```sh
docker compose run --rm pyspark \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/follow/08_rdds.py
```

## Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2025-11 data from the official website:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```


## 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

```sh
docker compose run --rm pyspark \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/homework/01.py
```

>>> 4.1.3

## 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB


## 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

- 62,610
- 102,340
- 162,604
- 225,768


## 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 22.7
- 58.2
- 90.6
- 134.5


## 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080



## 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

If multiple answers are correct, select any
