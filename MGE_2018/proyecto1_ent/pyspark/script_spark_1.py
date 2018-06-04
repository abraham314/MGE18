from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as functions
import pandas as pd
from pyspark.sql.functions import lit

# Corre todo en una 'linea', por si se estan repartiendo puntos extra :)

pd.DataFrame([[C_ACT.select(C_ACT.anio).rdd.flatMap(list).first(),\
  par,\
  C_ACT.filter(C_ACT.id_parameter == par).filter(C_ACT.value.isNotNull()).select(C_ACT.value.cast("float")).agg({"value": "min"}).collect()[0]["min(value)"] , \
  C_ACT.filter(C_ACT.id_parameter == par).filter(C_ACT.value.isNotNull()).select(C_ACT.value.cast("float")).agg({"value": "max"}).collect()[0]["max(value)"], \
  C_ACT.filter(C_ACT.id_parameter == par).filter(C_ACT.value.isNotNull()).select(C_ACT.value.cast("float")).agg({"value": "sum"}).collect()[0]["sum(value)"]  / len(C_ACT.filter(C_ACT.id_parameter == par).select(C_ACT.value).filter(C_ACT.value.isNull()).rdd.flatMap(lambda x: x).collect()) if len(C_ACT.filter(C_ACT.id_parameter == par).select(C_ACT.value).filter(C_ACT.value.isNull()).rdd.flatMap(lambda x: x).collect()) > 0 else 0, \
  C_ACT.filter(C_ACT.id_parameter == par).filter(C_ACT.value.isNotNull()).select(C_ACT.value.cast("float")).approxQuantile("value", [0.25], 0.2),\
  C_ACT.filter(C_ACT.id_parameter == par).filter(C_ACT.value.isNotNull()).select(C_ACT.value.cast("float")).approxQuantile("value", [0.5], 0.2), \
  C_ACT.filter(C_ACT.id_parameter == par).filter(C_ACT.value.isNotNull()).select(C_ACT.value.cast("float")).approxQuantile("value", [0.75], 0.2),\
  C_ACT.filter(C_ACT.id_parameter == par).filter(C_ACT.value.isNotNull()).select(C_ACT.value.cast("float")).agg({"value": "stddev"}).collect()[0]["stddev(value)"],\
  len(C_ACT.filter(C_ACT.id_parameter == par).select(C_ACT.value).filter(C_ACT.value.isNull()).rdd.flatMap(lambda x: x).collect())\
 ] \
 for par in C_ACT.select('id_parameter').distinct().rdd.flatMap(list).collect() \
 for C_ACT in [ sqlContext.read.load('datos/contaminantes_' + str(anio) + '.csv', format='com.databricks.spark.csv', header='true', inferSchema='true').withColumn("anio", lit(anio)) for anio in range(1986,2018)]], \
 columns = ['anio', 'parametro', 'minimo', 'maximo', 'media', 'q25', 'q50', 'q75', 'desviacion', 'num_nulos'])
