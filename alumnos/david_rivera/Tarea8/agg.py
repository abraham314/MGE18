import sys

reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import countDistinct

sc = SparkContext(appName="parquet")
spark = SparkSession(sc)

datos = spark.read.parquet("s3a://al102964-bucket1/tarea8/parquet")

datos.groupBy("categoria")\
     .agg(countDistinct("producto").alias('NumeroProductos'))\
     .coalesce(1)\
     .write.csv("s3a://al102964-bucket1/tarea8/salida", header=True)


