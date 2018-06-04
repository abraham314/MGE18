from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

prueba=spark.read.parquet('s3://oliabherrera/00_luigi/profeco.parquet')
cuenta=prueba.groupBy("_c1").count()
cuenta.show()
cuenta.write.csv('s3://oliabherrera/00_luigi/cuenta.csv')