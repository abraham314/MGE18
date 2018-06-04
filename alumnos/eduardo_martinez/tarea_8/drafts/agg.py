#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import pip
pip.main(['install',  "--user",  "pandas"])
import pandas as pd
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

#lectura del parquet
data = spark.read.parquet("s3a://jorge-altamirano/profeco/data.parquet")

#funcion que hace lo mismo que summary, pero 
#irónicamente más rápido que spark (esta es para numéricos)
def summary_j3a(col):
    min1 = data.select(min(data[col]).alias("min")).toPandas().transpose()
    max1 = data.select(max(data[col]).alias("max")).toPandas().transpose()
    avg1 = data.select(mean(data[col]).alias("avg")).toPandas().transpose()
    std1 = data.select(stddev(data[col]).alias("stddev")).toPandas().transpose()
    probs = [0.25, 0.5, 0.75]
    qnt1 = pd.DataFrame(  \
        data.approxQuantile(col, probabilities=probs, relativeError=0.05)
    )
    qnt1 = qnt1.rename_axis({0: "25%", 1: "50%", 2: "75%"}, axis=0)
    complete = min1.append(qnt1).append(max1).append(avg1).append(std1)
    complete = complete.rename(index=str, columns={0: col})
    complete[col] = complete.apply(lambda x: "{:,}".format(x[col]), axis=1)
    return complete
#funcion que hace lo mismo que summary, pero 
#irónicamente más rápido que spark (esta es para strings)
def summary_string_j3a(col):
    min1 = data.select(min(data[col]).alias("min")).toPandas().transpose()
    max1 = data.select(max(data[col]).alias("max")).toPandas().transpose()
    complete = min1.append(max1)
    complete = complete.rename(index=str, columns={0: col})
    return complete
summary_list = list()
#ahora lo vamos a hacer por cada columna
for (nombre, tipo) in data.dtypes:
    ## header de la columna
    print("####### %s #######"%nombre)
    ## los conteos los hacemos una vez
    cnt1 = pd.DataFrame([{"count": data.count()}]).transpose()
    cnt1 = cnt1.rename(index=str, columns={0: nombre})
    ## sacar output de una columna string
    if tipo in "string":
        ret_val = cnt1.append(summary_string_j3a(nombre))
        #agregarlo a un pandas para escritura al final
        summary_list.append(ret_val)
        print(ret_val)
    ## sacar output de una columna numérica
    elif "decimal" in tipo or tipo in "double":
        #agregarlo a un pandas para escritura al final
        ret_val = cnt1.append(summary_j3a(nombre))
        summary_list.append(ret_val)
        print(ret_val)
    #no sabemos qué es, entoncess sacamos sin datos :-)
    #se puede mejorar
    else:
        print("Sin sumario")
    print("\n")
sumario = pd.concat(summary_list, 1)
#sumario = sumario.applymap(lambda x: str(x).encode("utf-8") )
sumario_df = spark.createDataFrame(sumario.astype(str))
sumario_df.write.parquet("s3a://jorge-altamirano/profeco/sumario_df.parquet", 
                                    mode="overwrite")
sumario_df.show()

#fin python
