
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.types import Row

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


import numpy as np  # type: ignore[import]
import pandas as pd  # type: ignore[import]

# Generate a Pandas DataFrame
pdf = pd.DataFrame(np.random.rand(100, 3))
pdf_first_row = pdf.loc[[0]]

print(pdf_first_row)

# Create a Spark DataFrame from a Pandas DataFrame using Arrow
df = spark.createDataFrame(pdf)
df.show(1)

# Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
result_pdf = df.select("*").toPandas()

result_pdf_first_row = result_pdf.loc[[0]]

print(result_pdf_first_row)



