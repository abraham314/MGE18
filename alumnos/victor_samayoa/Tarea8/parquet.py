from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
df = spark.read.csv("s3://oliabherrera/00_luigi/profeco.csv")
df.write.parquet('s3://oliabherrera/00_luigi/profeco.parquet')