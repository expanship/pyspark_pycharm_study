from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.types import Row

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
schema = StructType([ \
    StructField("userid", StringType(), True), \
    StructField("tracktimestamp", TimestampType(), True), \
    StructField("musicbrainzartistid", StringType(), True), \
    StructField("artistname", StringType(), True), \
    StructField("musicbrainztrackid", StringType(), True), \
    StructField("trackname", StringType(), True)])

csvPath = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/data/single_text_file/userid-timestamp-artid-artname-traid-traname_10k.tsv"
     
df = spark \
    .read \
    .option("sep", "\t") \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX") \
    .schema(schema) \
    .csv(csvPath) \
    .select("userid","tracktimestamp","musicbrainzartistid","artistname","musicbrainztrackid","trackname")
                            
# Displays the content of the DataFrame to stdout
df.show()


# TEMP AND LOCAL
df.createOrReplaceTempView("tracks")
sqlDF = spark.sql("SELECT * FROM tracks where artistname = 'Deep Dish'")
sqlDF.show()

# TEMP BUT GLOBAL
# Global temporary view is tied to a system preserved database global_temp, and we must use the qualified name to refer it, e.g. SELECT * FROM global_temp.view1.
df.createGlobalTempView("tracksglobal")
sqlDFGlobal = spark.sql("SELECT * FROM global_temp.tracksglobal where artistname = 'Deep Dish'")
sqlDFGlobal.show()


