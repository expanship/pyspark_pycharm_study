from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import Row

master = 'local[*]'
appName = 'rdd examples 1'
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

broadcastVar = sc.broadcast([1, 2, 3])

print("Broadcast variable is %s" % broadcastVar.value)