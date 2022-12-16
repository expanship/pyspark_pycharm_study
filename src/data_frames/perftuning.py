from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.types import Row

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

schema = StructType([ \
    StructField("userid", StringType(), True), \
    StructField("tracktimestamp", TimestampType(), True), \
    StructField("musicbrainzartistid", StringType(), True), \
    StructField("artistname", StringType(), True), \
    StructField("musicbrainztrackid", StringType(), True), \
    StructField("trackname", StringType(), True)])

csvPath = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/single_text_file/userid-timestamp-artid-artname-traid-traname_10k.tsv"
     
df = spark \
    .read.option("sep", "\t") \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX") \
    .schema(schema) \
    .csv(csvPath) \
    .select("userid","tracktimestamp","musicbrainzartistid","artistname","musicbrainztrackid","trackname")
    
df.cache()

df.show()
            
            
            