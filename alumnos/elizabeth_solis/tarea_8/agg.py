# Importamos las librerias necesarias para nuestro análisis descriptivo
import pyspark 
import numpy as np
from pyspark.sql.functions import col,sum, countDistinct
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, avg

sc = SparkContext('local')

sqlContext = SQLContext(sc)

allData = sqlContext.read.parquet('s3://luigiclasemge/output.parquet')
datos = allData.groupBy("categoria")\
                              .agg(avg("precio").alias('prom'))\
                              .coalesce(1)\
                              .write.csv('s3://luigiclasemge/salida/', header=True)
