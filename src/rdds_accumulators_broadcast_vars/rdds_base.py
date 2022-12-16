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


# base rdd
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
print(distData.take(4))

# import from single text file
single_file_text = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/single_text_file/bible.txt"  # Should be some file on your system
singeFileRdd = sc.textFile(single_file_text)
print("Single file number of lines, %d" % singeFileRdd.count())

# import multiple files at same time
multi_file_text = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/multi_text_files/*.txt"  # Should be some file on your system
multiFileRdd = sc.textFile(multi_file_text)
print("Multi file number of lines, %d" % multiFileRdd.count())

# whole text files
whole_files_rdd = sc.wholeTextFiles(multi_file_text, 2, True)
print("whole file number of lines, %d" % whole_files_rdd.count())
print("whole file first content, %s" % whole_files_rdd.take(1))

# save to pickle format
whole_files_rdd.toDF().coalesce(1).write().overwrite().saveAsPickleFile("/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/output_pickle")

# save a sequnce file

rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
rdd.coalesce(1).write().overwrite().saveAsSequenceFile("home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/seq1")
sorted(sc.sequenceFile("home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/seq2").collect())

# NOTE: Arrays are not handled out-of-the-box. Users need to specify custom ArrayWritable subtypes when reading or writing

# RDD OPERATIONS

lines = sc.textFile(single_file_text)

# transformations lazily executed
lineLengths = lines.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)

# transformation do not run until an action occurs
lineLengths.cache()
# lineLengths.persist()

#


