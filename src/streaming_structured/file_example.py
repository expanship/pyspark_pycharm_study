from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, TimestampType
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from click.decorators import option

# Can only run this once. restart your kernel for any errors.

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# schema is required for dataframe streaming to ensure consistency

schema = StructType([ \
    StructField("userid", StringType(), True), \
    StructField("tracktimestamp", TimestampType(), True), \
    StructField("musicbrainzartistid", StringType(), True), \
    StructField("artistname", StringType(), True), \
    StructField("musicbrainztrackid", StringType(), True), \
    StructField("trackname", StringType(), True)])

# DIRECTORY STRUCTURE LIMITATIONS

# Partition discovery does occur when subdirectories that are named /key=value/ are present and 
# listing will automatically recurse into these directories. If these columns appear in the 
# user-provided schema, they will be filled in by Spark based on the path of the file being read. 
# The directories that make up the partitioning scheme must be present when the query starts and 
# must remain static. For example, it is okay to add /data/year=2016/ when /data/year=2015/ was 
# present, but it is invalid to change the partitioning column (i.e. by creating the directory 
# /data/date=2016-04-17/).
csvPath = "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/data/test_stream_toprocess_files/*.tsv"

## Added option to archive data to different folder
csvDF = spark \
    .readStream \
    .option("sep", "\t") \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX") \
    .option("cleanSource", "archive") \
    .option("sourceArchiveDir", "/home/rdelorimier/eclipse-workspace/MyPythonSparkProject/data/test_stream_processed_files" ) \
    .schema(schema) \
    .csv(csvPath)
    
artists = csvDF.groupBy("firstName").count()
    
 # Start running the query that prints the running counts to the console
query = artists \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

# NOTES

# path: path to the input directory, and common to all file formats.
# 
# maxFilesPerTrigger: maximum number of new files to be considered in every trigger (default: no max)

# latestFirst: whether to process the latest new files first, useful when there is a large backlog of files (default: false)

# fileNameOnly: whether to check new files based on only the filename instead of on the full path (default: false). With this set to `true`, the following files would be considered as the same file, because their filenames, "dataset.txt", are the same:
# "file:///dataset.txt"
# "s3://a/dataset.txt"
# "s3n://a/b/dataset.txt"
# "s3a://a/b/c/dataset.txt"

# maxFileAge: Maximum age of a file that can be found in this directory, before it is ignored. For the first batch all files will be considered valid. If latestFirst is set to `true` and maxFilesPerTrigger is set, then this parameter will be ignored, because old files that are valid, and should be processed, may be ignored. The max age is specified with respect to the timestamp of the latest file, and not the timestamp of the current system.(default: 1 week)

# cleanSource: option to clean up completed files after processing.
# Available options are "archive", "delete", "off". If the option is not provided, the default value is "off".
# When "archive" is provided, additional option sourceArchiveDir must be provided as well. The value of "sourceArchiveDir" must not match with source pattern in depth (the number of directories from the root directory), where the depth is minimum of depth on both paths. This will ensure archived files are never included as new source files.
# For example, suppose you provide '/hello?/spark/*' as source pattern, '/hello1/spark/archive/dir' cannot be used as the value of "sourceArchiveDir", as '/hello?/spark/*' and '/hello1/spark/archive' will be matched. '/hello1/spark' cannot be also used as the value of "sourceArchiveDir", as '/hello?/spark' and '/hello1/spark' will be matched. '/archived/here' would be OK as it doesn't match.
# Spark will move source files respecting their own path. For example, if the path of source file is /a/b/dataset.txt and the path of archive directory is /archived/here, file will be moved to /archived/here/a/b/dataset.txt.
# 
# NOTE: Both archiving (via moving) or deleting completed files will introduce overhead (slow down, even if it's happening in separate thread) in each micro-batch, so you need to understand the cost for each operation in your file system before enabling this option. On the other hand, enabling this option will reduce the cost to list source files which can be an expensive operation.
# Number of threads used in completed file cleaner can be configured with spark.sql.streaming.fileSource.cleaner.numThreads (default: 1).
# NOTE 2: The source path should not be used from multiple sources or queries when enabling this option. Similarly, you must ensure the source path doesn't match to any files in output directory of file stream sink.
# NOTE 3: Both delete and move actions are best effort. Failing to delete or move files will not fail the streaming query. Spark may not clean up some source files in some circumstances - e.g. the application doesn't shut down gracefully, too many files are queued to clean up.

# For file-format-specific options, see the related methods in DataStreamReader (Scala/Java/Python/R). E.g. for "parquet" format options see DataStreamReader.parquet().

# In addition, there are session configurations that affect certain file-formats. See the SQL Programming Guide for more details. E.g., for "parquet", see Parquet configuration section.





