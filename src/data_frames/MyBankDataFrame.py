# Imports the PySpark libraries
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
 
# The File System path of the CSV file 'bank.csv'
# (for local File System, without YARN)
bankCsvFile='/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/data/bank/bank.csv'
 
# The HDFS URL of the CSV file 'bank.csv'
# (for hadoop access, with or without YARN),
# where '192.168.1.11:8020' is the NameNode URL,
# and '/user/hadoop/bank.csv' is the HDFS path
#bankCsvFile='hdfs://192.168.1.11:8020/user/hadoop/bank.csv'
 
# Configure the Spark context to give a name to the application
sparkConf = SparkConf().setAppName("MyBankDataFrame")
sc = SparkContext(conf = sparkConf)
 
# Gets the Spark SQL context
sqlContext = SQLContext(sc)
 
# Loads the CSV file as a Spark DataFrame
df_bank = sqlContext.read.option("delimiter", ";").option("header", True).csv(bankCsvFile)
 
# Shows the CSV schema (not updated yet)
df_bank.printSchema()
 
# Updates the schema: Casts the type for columns containing integers
df_bank = df_bank \
.withColumn('age', df_bank.age.cast('int')) \
.withColumn('balance', df_bank.balance.cast('int')) \
.withColumn('day', df_bank.day.cast('int')) \
.withColumn('duration', df_bank.duration.cast('int')) \
.withColumn('campaign', df_bank.campaign.cast('int')) \
.withColumn('pdays', df_bank.pdays.cast('int')) \
.withColumn('previous', df_bank.previous.cast('int'))
 
# Shows the new schema updated
df_bank.printSchema()
 
# Shows the 10 first rows
df_bank.show(10)
 
# Associates the Bank DataFrame with the table name 'bank'
sqlContext.registerDataFrameAsTable(df_bank, 'bank')
 
# SQL query:
# The list of people with age under 41, sorted by age and containing the frequencies
query = 'select age, count(1) freq from bank where age > 41 group by age order by age'
 
# Execute the SQL query
df_result = sqlContext.sql(query)
 
# Shows the 10 first rows of the result
df_result.show(10)
 
# Shows the BarChart diagram
# Please comment this code below if you didn't install "MathPlotLib" before
import matplotlib.pyplot as plt
plt.xlabel('Ages')
plt.ylabel('Frequencies')
dfp_result = df_result.toPandas() # Convert the Spark DataFrame to a Pandas DataFrame
plt.bar(dfp_result.age, dfp_result.freq)
plt.show()