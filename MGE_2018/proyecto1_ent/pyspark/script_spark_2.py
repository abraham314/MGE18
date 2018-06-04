from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as functions
import pandas as pd
from pyspark.sql.functions import lit
import matplotlib.colors as colors
import matplotlib.cm as cmx

dataframe_collection = {} 
# De aquí sólo tomamos las estaciones
estaciones = sqlContext.read.load('datos/contaminantes_2005.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')

# Aquí sucede la magia, se guardan en un data frame separado.
for estacion in estaciones.select(estaciones.id_station).distinct().rdd.flatMap(list).collect():
    dataframe_collection[estacion] = pd.DataFrame([[C_ACT.select(C_ACT.anio).rdd.flatMap(list).first(),\
      estacion, \
      par,\
      C_ACT.filter((C_ACT.id_parameter == par) & (C_ACT.id_station == estacion)).filter(C_ACT.value.isNotNull()).select(C_ACT.value.cast("float")).agg({"value": "min"}).collect()[0]["min(value)"] , \
      C_ACT.filter((C_ACT.id_parameter == par) & (C_ACT.id_station == estacion)).filter(C_ACT.value.isNotNull()).select(C_ACT.value.cast("float")).agg({"value": "max"}).collect()[0]["max(value)"], \
      C_ACT.filter((C_ACT.id_parameter == par) & (C_ACT.id_station == estacion)).filter(C_ACT.value.isNotNull()).select(C_ACT.value.cast("float")).agg({"value": "sum"}).collect()[0]["sum(value)"],\
      C_ACT.filter((C_ACT.id_parameter == par) & (C_ACT.id_station == estacion)).filter(C_ACT.value.isNotNull()).select(C_ACT.value.cast("float")).agg({"value": "stddev"}).collect()[0]["stddev(value)"]\
     ] \
     for par in C_ACT.select('id_parameter').distinct().rdd.flatMap(list).collect() \
     for C_ACT in [ sqlContext.read.load('datos/contaminantes_' + str(anio) + '.csv', format='com.databricks.spark.csv', header='true', inferSchema='true').withColumn("anio", lit(anio)) for anio in range(2005,2008)]])#, \
     #columns = ['anio', 'estacion', 'parametro', 'minimo', 'maximo', 'total', 'desviacion'])


# Impresion de gráficas

for key in dataframe_collection.keys():
    print("\n" +"="*40)
    print(key)
    #print("-"*40)
    df = dataframe_collection[key].dropna()
    uniq = list(set(df[2]))
    #print(uniq)
    z = range(1,len(uniq))
    hot = plt.get_cmap('hot')
    cNorm  = colors.Normalize(vmin=0, vmax=len(uniq))
    scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=hot)

    for i in range(len(uniq)):
        indx = df[2] == uniq[i]
        plt.scatter(df[0].loc[indx], df[5].loc[indx], s=10, color=scalarMap.to_rgba(i), label=uniq[i])
    print(df[1].iloc[0])
    plt.title = df[1].iloc[0]
    plt.xlabel('Año')
    plt.ylabel('Total')
    plt.legend(loc='upper left')
    plt.show()
