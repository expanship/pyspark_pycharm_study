# Pyspark Pycharm Project

## Requirements

### Spark

You will need to have spark set up on the laptop. Do these steps

1. Create an Applications folder in your home directory
1. Download spark from https://spark.apache.org/downloads.html, and choose "Pre-built for Apache Hadoop 3.3 and later"
1. Untar the file and move it to the Applications folder
1. Add some lines to your shell profile (.zprofile):
    ```
    # SPARK BASE VARIABLES
    export SPARK_HOME=/Users/rdelorimier/Applications/spark-3.2.1-bin-hadoop3.2
    export PATH=$PATH:$SPARK_HOME/bin

    # VARIABLE FOR PYSPARK
    export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.10.9.3-src.zip:$PYTHONPATH
    ```

### Pycharm Setup

If you are using pycharm for development, you will need to do these steps to set it up

1. Go to Preferences
2. Go to Project > Project Structure
3. Click the "+ Add Content Root" button
4. Add two entries:
    - The main python module in spark: 
        - ~/Application/Applications/spark-3.2.1-bin-hadoop3.2/python
    - The py4j zip file, so python can run in spark:
        - ~/Application/Applications/spark-3.2.1-bin-hadoop3.2/python/lib/py4j-0.10.3-src.zip
        - Py4J enables Python programs running in a Python interpreter to dynamically access Java objects in a Java Virtual Machine
