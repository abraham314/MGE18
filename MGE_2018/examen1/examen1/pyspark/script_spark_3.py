from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as functions
import pandas as pd
from pyspark.sql.functions import lit
import matplotlib.colors as colors
import matplotlib.cm as cmx

proms = {}

for par in C_ACT.select(C_ACT.id_parameter).distinct().rdd.flatMap(list).collect():
    C_ACT = sqlContext.read.load('s3a://pregunta4/*.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
    C_ACT_conDIA = C_ACT.filter(C_ACT.id_parameter == par).withColumn("anio_mes_dia", functions.concat(functions.expr("substring(date, 7, 4)"), functions.concat(functions.expr("substring(date, 3, 4)"),functions.expr("substring(date, 1, 2)"))))
    #C_ACT_conDIA.show()
    promedios = C_ACT_conDIA.groupby('anio_mes_dia').avg('value').sort('anio_mes_dia')
    #promedios.show()
    con_lag = promedios.withColumn('id_parameter', lit('CO')).withColumn('promedio_previo',
                                  functions.lag(promedios['avg(value)'])
                                                  .over(Window.partitionBy("id_parameter").orderBy('avg(value)')))

    result = con_lag.withColumn('cambio_dia',
                               (con_lag['avg(value)'] - con_lag['promedio_previo'] / con_lag['avg(value)']))
    #result.show()
    proms[par] = result.filter(result.cambio_dia.isNotNull()).select(functions.avg(result.cambio_dia)).rdd.flatMap(list).first()

# Impresion de resultados

proms = sorted(proms, key=lambda x: x[1], reverse=True)
for key in proms:
    print("\n" +"="*40)
    print(key)

