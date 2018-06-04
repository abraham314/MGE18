from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as functions
import pandas as pd
from pyspark.sql.functions import lit
import matplotlib.colors as colors
import matplotlib.cm as cmx

tabla = sqlContext.read.load('datos/contaminantes_*.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
con_mes_hora = tabla.filter(tabla.id_parameter == 'PM2.5').withColumn("mes", functions.expr("substring(date, 4, 2)")).withColumn("hora", functions.expr("substring(date, 12, 2)"))
#con_mes_hora.filter(con_mes_hora.mes == "02").show()
con_mes_hora.select(con_mes_hora.mes).distinct().rdd.flatMap(list).collect()
#con_mes_hora.groupby('hora').agg(functions.avg('value').alias('avg_value')).sort('hora').show(40)

horario_por_mes = {}
for mes_actual in ["01","02","03","04","05","06","07","08","09","10","11","12"]:
    horario_por_mes[mes_actual] = pd.DataFrame(con_mes_hora.filter(con_mes_hora.mes == mes_actual).groupby('hora').agg(functions.avg('value').alias('avg_value')).sort('hora').collect())



# Impresion de resultados

import matplotlib.colors as colors
import matplotlib.cm as cmx
i = 0
hot = plt.get_cmap('cool_r')
cNorm  = colors.Normalize(vmin=0, vmax=12)
scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=hot)
for key in horario_por_mes.keys():
    df = horario_por_mes[key].dropna()
    plt.scatter(df[0], df[1], color=scalarMap.to_rgba(i), label=key)
    i += 1
plt.xlabel('Año')
plt.ylabel('Total')
plt.legend(loc='upper left')
plt.show()

