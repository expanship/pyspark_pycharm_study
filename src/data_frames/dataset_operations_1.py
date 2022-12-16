"""SimpleApp.py"""
from pyspark.sql import SparkSession

bible_text = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/single_text_file/bible.txt"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(bible_text).cache()

print("Lines total count %d " % logData.count())

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()