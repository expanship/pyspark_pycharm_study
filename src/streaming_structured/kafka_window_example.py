from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, TimestampType, MapType
from pyspark.sql.functions import split, from_json, explode, window, col
from pyspark.sql.types import *
from click.decorators import option
from pyspark.sql.window import Window

# SET UP 
# Requirements
#
# Jars
# - commons-pool2-2.11.1.jar
# - kafka-clients-2.2.0.jar
# - spark-sql-kafka-0-10_2.12-3.2.1.jar
# - spark-token-provider-kafka-0-10_2.12-3.2.1.jar
# These jars need to be passed as part of the PYSPARK_SUBMIT_ARGS variable. To set up, go to Window > Preferences > PyDev > Interpreters > Python Interpreter > Environment tab
# --master local[*] --queue PyDevSpark3.2.2 --jars /home/rdelorimier/apps/spark-3.2.1-bin-hadoop3.2/lib-external/spark-sql-kafka-0-10_2.12-3.2.1.jar,/home/rdelorimier/apps/spark-3.2.1-bin-hadoop3.2/lib-external/kafka-clients-2.2.0.jar,/home/rdelorimier/apps/spark-3.2.1-bin-hadoop3.2/lib-external/commons-pool2-2.11.1.jar,/home/rdelorimier/apps/spark-3.2.1-bin-hadoop3.2/lib-external/spark-token-provider-kafka-0-10_2.12-3.2.1.jar pyspark-shell
#
# Kafka
# Download a version of kakfa locally and start it up:
# $ bin/zookeeper-server-start.sh config/zookeeper.properties
# $ bin/kafka-server-start.sh config/server.properties
# $ bin/kafka-topics.sh --create --topic spark-events --bootstrap-server localhost:9092
#
# Message Util
# The scala gradle project, de-kakfa-message-util, is found in the utils folder under this project. Create an uber jar:
# > gradle shadowJar
# Then run the jar to put messages on the topic:
# $ cd /home/rdelorimier/eclipse-workspace/MyPythonSparkProject/utils/de-kakfa-message-util/build/libs/
# $ java -cp kakfa-message-util-0.0.1-all.jar us.gloo.bdp.de.json.confluent.dynamic.ScalaJsonConfluentProducerDelayDynamicAutogenerate -n /home/rdelorimier/eclipse-workspace/MyPythonSparkProject/utils/de-kakfa-message-util/for_testing/producer.properties -m /home/rdelorimier/eclipse-workspace/MyPythonSparkProject/utils/de-kakfa-message-util/for_testing/producer.properties
#
# Start the pyspark app to capture the windowed messages:
#
# Batch: 69
# -------------------------------------------
# +--------------------+------------+-----+
# |              window|  entityType|count|
# +--------------------+------------+-----+
# |{2022-02-24 17:05...|Organization|   20|
# |{2022-02-24 17:00...|    Audience|   31|
# |{2022-02-24 17:05...|    Audience|   31|
# |{2022-02-24 17:00...|      Person|   19|
# |{2022-02-24 17:05...|      Person|   19|
# |{2022-02-24 17:00...|Organization|   20|
# +--------------------+------------+-----+
#
#




spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# schema is required for dataframe streaming to ensure consistency

schema = StructType([ \
    StructField("entityType", StringType(), True), \
    StructField("sourceType", StringType(), True), \
    StructField("stage", StringType(), True), \
    StructField("createTimestamp", StringType(), True), \
    StructField("event", StringType(), True), \
    StructField("zipcode", StringType(), True), \
    StructField("metadata", MapType(StringType(),StringType(),False), True), \
    StructField("payload", MapType(StringType(),StringType(),False), True)])
                      
                               
# Subscribe to 1 topic
messagesDF = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "spark-events") \
  .load() 

messagesDF = messagesDF.withColumn("stringval", col("value").cast("string"))
messagesDF.printSchema()

messagesWithRecord = messagesDF.select("timestamp", from_json(messagesDF.stringval, schema).alias("json"))

messagesWithRecord = messagesWithRecord.select("timestamp", "json.*")

messagesWithRecord.printSchema()
  
windowedCounts = messagesWithRecord.groupBy(
    window(messagesWithRecord.timestamp, "10 minutes", "5 minutes"),
    messagesWithRecord.entityType
).count()
    
 # Start running the query that prints the running counts to the console
query = windowedCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()


