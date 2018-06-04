#!/usr/bin/env python
from __future__ import print_function
import sys
import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import *
from pyspark.sql import DataFrameStatFunctions, DataFrame
from pyspark.sql.types import *

#inicializar cluster
conf = SparkConf()
#conf.set("spark.driver.memory", "16g")
#conf.set("spark.driver.cores", 4)
#conf.set("spark.driver.memoryOverhead", 0.9)
#conf.set("spark.executor.memory", "32g")
#conf.set("spark.executor.cores", 12)
#conf.set("spark.jars", "/home/jaa6766")
sc = SparkContext(master = "local[*]", sparkHome="/usr/local/spark/",
                  appName="tarea-mge-8-parqueteo", conf=conf)
spark = SQLContext(sc)

#leer csv fuente original
data = spark.read.csv("s3a://jorge-altamirano/profeco/data.csv", 
                      schema = StructType() \
                        .add("producto", StringType(), False) \
                        .add("presentacion", StringType(), True) \
                        .add("marca", StringType(), True) \
                        .add("categoria", StringType(), True) \
                        .add("catalogo", StringType(), True) \
                        .add("precio", DecimalType(precision=16, scale=4), True) \
                        .add("fechaRegistro", TimestampType(), True) \
                        .add("cadenaComercial", StringType(), True) \
                        .add("giro", StringType(), True) \
                        .add("nombreComercial", StringType(), True) \
                        .add("direccion", StringType(), True) \
                        .add("estado", StringType(), True) \
                        .add("municipio", StringType(), True) \
                        .add("latitud", DoubleType(), True) \
                        .add("longitud", DoubleType(), True),
                      inferSchema=False, 
                      escape='"',
                      quote='"',
                      timestampFormat="yyyy-MM-dd hh:mm:ss",
                      header=True)
#escribir a parquet
data.coalesce(1).write.parquet("s3a://jorge-altamirano/profeco/data.parquet", mode="overwrite")

#fin python
