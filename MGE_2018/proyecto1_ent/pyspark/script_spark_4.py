from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as functions
import pandas as pd
from pyspark.sql.functions import lit
import matplotlib.colors as colors
import matplotlib.cm as cmx

tabla = sqlContext.read.load('s3a://pregunta4/contaminantes_*.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
con_mes = tabla.filter(tabla.id_parameter == 'PM2.5').withColumn("mes", functions.expr("substring(date, 4, 2)"))
con_mes.groupby('mes').agg(functions.avg('value').alias('avg_value')).sort('mes').show()
