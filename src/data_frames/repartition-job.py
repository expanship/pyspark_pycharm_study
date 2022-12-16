import sys
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import *
from pyspark.sql import SparkSession

source=sys.argv[1]
target=sys.argv[2]

spark = SparkSession\
    .builder\
    .appName("repartition-job")\
    .getOrCreate()

spark.read.json(source) \
  .withColumn("timestamp", to_timestamp(col("time"))) \
  .select( \
     col("main.temp").alias("temperature"), \
     col("main.humidity").alias("humidity"), \
     col("main.pressure").alias("pressure"), \
     col("time").alias("event_ts"), \
     hour(col("timestamp")).alias("hour"), \
     dayofmonth(col("timestamp")).alias("day"), \
     month(col("timestamp")).alias("month"), \
     year(col("timestamp")).alias("year")) \
  .repartition("year", "month", "day", "hour") \
  .write \
  .partitionBy("year", "month", "day", "hour") \
  .mode("overwrite") \
  .parquet(target)

spark.stop()

