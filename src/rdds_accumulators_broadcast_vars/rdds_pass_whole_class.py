from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import Row


single_file_text = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/single_text_file/bible.txt"  # Should be some file on your system

# THIS IS SUBOPTIMAL AND WILL PASS THE WHOLE CLASS
# SHOULD PASS ONLY NEEDED OBJECTS

# This appears to fail

class MyClass(object):
    def func(self, s):
        words = s.split(" ")
        return len(words)
    def doStuff(self, rdd):
        return sc.textFile(single_file_text).map(self.func)
    
    

"""MyScript.py"""
if __name__ == "__main__":
    
    master = 'local[*]'
    appName = 'rdd examples 1'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    
    testclass = MyClass()
    rdd = sc.textFile(single_file_text)
    words = rdd.map(testclass.doStuff)
    print("Number of word lengths %d" % words.count())
    