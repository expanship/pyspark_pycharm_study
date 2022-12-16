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
    
single_file_text = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/src/data_files/single_text_file/bible.txt"  # Should be some file on your system

#
# THIS IS VERY SUSPECT, AS IT WILL BE OUT OF THE SCOPE OF FOREACH
#
counter = 0
rdd = sc.textFile(single_file_text)

# Wrong: Don't do this!!
def increment_counter_buggy(x):
    global counter
    counter += 1
    
rdd.foreach(increment_counter_buggy)

# COULD BE ANYTHING
print("Bugger counter value: ", counter)

#
# CORRECT WAY WITH ACCUMULATOR
#

accum = sc.accumulator(0)

def increment_counter_accum(x):
    accum.add(1)
    
rdd.foreach(increment_counter_accum)

# COULD BE ANYTHING
print("Accumulator counter value: ", accum.value)






