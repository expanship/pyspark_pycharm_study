from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import Row

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
sc = spark.sparkContext

csvPath = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/single_text_file/userid-timestamp-artid-artname-traid-traname_10k.tsv"

tracklines = sc.textFile(csvPath)
tracktsv = tracklines.map(lambda l: l.split("\t"))
trackcols = tracktsv.map(lambda p: Row(userid=p[0], tracktimestamp=p[1], trackname=p[5]))
tracksDF = spark.createDataFrame(trackcols)
tracksDF.show()



