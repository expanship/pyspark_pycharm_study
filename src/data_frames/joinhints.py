from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.types import Row
from pyspark.sql.functions import col
import time

def current_milli_time():
    return round(time.time() * 1000)

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
#
# FOR TUNING
# SEE: https://spark.apache.org/docs/latest/sql-performance-tuning.html
#


# Tuning caching Data In Memory

spark.conf.set('spark.sql.inMemoryColumnarStorage.compressed', True)
spark.conf.set('spark.sql.inMemoryColumnarStorage.batchSize', 1000)

trackSchema = StructType([ \
    StructField("userid", StringType(), True), \
    StructField("tracktimestamp", TimestampType(), True), \
    StructField("musicbrainzartistid", StringType(), True), \
    StructField("artistname", StringType(), True), \
    StructField("musicbrainztrackid", StringType(), True), \
    StructField("trackname", StringType(), True)])

tracksPath = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/single_text_file/userid-timestamp-artid-artname-traid-traname_10k.tsv"
     
trackDf = spark \
    .read.option("sep", "\t") \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX") \
    .schema(trackSchema) \
    .csv(tracksPath) \
    .select("userid","tracktimestamp","musicbrainzartistid","artistname","musicbrainztrackid","trackname")
    
trackDf.createOrReplaceTempView("tracks")

usersPath = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/single_text_file/userid-profile.tsv"
    
userSchema = StructType([ \
    StructField("userid", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("age", IntegerType(), True), \
    StructField("country", StringType(), True), \
    StructField("registered", StringType(), True)])

# usersDf = spark \
#     .read.option("sep", "\t") \
#     .option("header", True) \
#     .schema(userSchema) \
#     .csv(usersPath)
    
usersDf = spark \
    .read.option("sep", "\t") \
    .option("header", True) \
    .csv(usersPath)
    
    
usersDf = usersDf.withColumn("userid", col('#id'))
usersDf = usersDf.drop("#id")
    
# usersDf.show()

usersDf.createOrReplaceTempView("users")

startTime = current_milli_time()
spark.sql("""select /*+ BROADCAST(u) */ u.country, count(u.country)  
from tracks t, users u 
where t.userid = u.userid
group by u.country
order by count(u.country) desc""")
endTime = current_milli_time()
print("BROADCAST hint join took %d milliseconds" % (endTime - startTime))

startTime = current_milli_time()
spark.sql("""select /*+ BROADCASTJOIN(u) */ u.country, count(u.country)  
from tracks t, users u 
where t.userid = u.userid
group by u.country
order by count(u.country) desc""")
endTime = current_milli_time()
print("BROADCASTJOIN hint join took %d milliseconds" % (endTime - startTime))

startTime = current_milli_time()
spark.sql("""select /*+ MAPJOIN(t) */ u.country, count(u.country)  
from tracks t, users u 
where t.userid = u.userid
group by u.country
order by count(u.country) desc""")
endTime = current_milli_time()
print("MAPJOIN hint join took %d milliseconds" % (endTime - startTime))

startTime = current_milli_time()
spark.sql("""select /*+ SHUFFLE_MERGE(u) */ u.country, count(u.country)  
from tracks t, users u 
where t.userid = u.userid
group by u.country
order by count(u.country) desc""")
endTime = current_milli_time()
print("SHUFFLE_MERGE hint join took %d milliseconds" % (endTime - startTime))

startTime = current_milli_time()
spark.sql("""select /*+ MERGEJOIN(t) */ u.country, count(u.country)  
from tracks t, users u 
where t.userid = u.userid
group by u.country
order by count(u.country) desc""")
endTime = current_milli_time()
print("MERGEJOIN hint join took %d milliseconds" % (endTime - startTime))

startTime = current_milli_time()
spark.sql("""select /*+ MERGE(u) */ u.country, count(u.country)  
from tracks t, users u 
where t.userid = u.userid
group by u.country
order by count(u.country) desc""")
endTime = current_milli_time()
print("MERGE hint join took %d milliseconds" % (endTime - startTime))

startTime = current_milli_time()
spark.sql("""select /*+ SHUFFLE_HASH(u) */ u.country, count(u.country)  
from tracks t, users u 
where t.userid = u.userid
group by u.country
order by count(u.country) desc""")
endTime = current_milli_time()
print("SHUFFLE_HASH hint join took %d milliseconds" % (endTime - startTime))

startTime = current_milli_time()
spark.sql("""select /*+ SHUFFLE_REPLICATE_NL(u) */ u.country, count(u.country)  
from tracks t, users u 
where t.userid = u.userid
group by u.country
order by count(u.country) desc""")
endTime = current_milli_time()
print("SHUFFLE_REPLICATE_NL hint join took %d milliseconds" % (endTime - startTime))

startTime = current_milli_time()
spark.sql("""select /*+ BROADCAST(u), MERGE(t) */ u.country, count(u.country)  
from tracks t, users u 
where t.userid = u.userid
group by u.country
order by count(u.country) desc""")
endTime = current_milli_time()
print("SHUFFLE_REPLICATE_NL hint join took %d milliseconds" % (endTime - startTime))

    

