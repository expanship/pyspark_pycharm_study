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
    
# SET UP BASE DATA
bible_text = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/single_text_file/bible.txt"  # Should be some file on your system
bible_lines = sc.textFile(bible_text)
bible_lines = bible_lines.map(lambda l: l.replace('\t', ' '))
bible_words = bible_lines.flatMap(lambda l: l.split(" "))
bible_words_tuples = bible_words.map(lambda word: (word, 1)).cache()

mahabharata_text = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/single_text_file/mahabharata.txt"  # Should be some file on your system
mahabharata_lines = sc.textFile(mahabharata_text)
mahabharata_lines = mahabharata_lines.map(lambda l: l.replace('\t', ' '))
mahabharata_words = mahabharata_lines.flatMap(lambda l: l.split(" "))
mahabharata_words_tuples = mahabharata_words.map(lambda word: (word, 1)).cache()

# REDUCE BY KEY

reduceByKeyWordCounts = bible_words_tuples.reduceByKey(lambda a, b: a + b)
print("Reduce by key counts per word are %s" % reduceByKeyWordCounts.take(10))

# GROUP BY KEY, GENERALLY NOT SUGGESTED
# breakdow, first groups into key -> array, then mapvalues finds the list size to change to key -> size
groupByKeyWordCounts = bible_words_tuples.groupByKey().mapValues(len)
print("Group by key counts per word are %s" % groupByKeyWordCounts.take(10))

# FILTER

startsWithAWords = bible_words.filter(lambda l: l.startswith('A'))
print("Words filter to start with A %s" % startsWithAWords.take(10))

# JOIN 

join_rdd = bible_words_tuples.join(mahabharata_words_tuples)
print("Joined tuple rdd %s" % join_rdd.take(10))









