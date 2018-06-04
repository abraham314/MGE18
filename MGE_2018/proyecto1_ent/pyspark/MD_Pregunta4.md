
# Proyecto de Métodos de Gran Escala
## Pregunta 4

### Levantamiento de tecnologías
![](./pyspark/img/cluster_corriendo.png)

![](./pyspark/img/zeppelin_corriendo.png)

Para el presente ejercicio levantamos un cluster en AWS con 3 nodos. (Con Spark y Zeppelin por supuesto)
<br>
<img width="70%" src = "img/cluster_corriendo.png"/>
<br><br>
Y después montamos zeppelin desde nuestro navegador abriendo un "canal" vía la terminal.
<br>
<img width="70%" src = "img/zeppelin_corriendo.png"/>
<br>
<br>
Y cargamos los datos (vía S3 primero).
<br>
<img width="70%" src = "img/carga_datos.png"/>

### D.1 Realiza un pequeño profiling a los datos por métrica/año indicando: min, max, mean, approximateQuantiles, desviación estándar, número de datos sin registro (nulos, vacíos). 

Para éste ejercicio, hicimos uso de la programación funcional y la recursión de Spark/Pyspark/Python y cargamos todo en una sola 'linea de comando' a un dataframe de pandas, ordenado primero por parámetro (métrica) y luego por año agregando minimo, maximo, media, cuantiles, desviación y nulos . Aquí la imagen preliminar del output:<br>
**El código de la consulta está en el archivo <b>pregunta4/script_spark_1.py</b>.
<br>
<img width="70%" src="img/resultado_a_1.png"/>
<img width="30%" src="img/resultado_a_2.png"/>

Y luego guardamos el output en archivos distribuídos:<br>
<img width="70%" src = "img/33_outputs_a.png"/>
<br><br>
** Los archivos output están en la carpeta <b>pregunta4/output_a</b> con los nombres cambiados como fué requerido.

### D.2 ¿Existe evidencia que sugiera que el metrobus ayudó a reducir contaminantes en ciertas estaciones meteorológicas? (No hace falta construir un modelo, qué nos dice la estadística descriptiva??)

Se corrió la consula para almacenar los resultados por sucursal y contaminante en los años 2005, 2006 y 2007 para ver qué contaminantes bajaban en qué sucursales y así poder <b>sospechar</b> de alguna mejora, más no concluír.
<br><br>
<img width = "60%" src = "img/pregunta_4_b.png"/>

### Los resultados para cada estación fueron los siguientes:

<img width="50%" src = "img/ARA_b.png"/>
<br>
Podemos ver que SO2 y CO2 presentan evidencia de decrecimiento en estación: <b>ARA</b>.
<img width="50%" src = "img/ATI_b.png"/>
<br>
Podemos ver que SO2 presenta evidencia de decrecimiento en estación: <b>ATI</b>.
<img width="50%" src = "img/CAM_b.png"/>
<br>
Podemos ver que no hay evidencia de decrecimiento en estación: <b>CAM</b>.
<img width="50%" src = "img/COY_b.png"/>
<br>
Podemos ver que hay evidencia de decrecimiento en PM2.5 en estación: <b>COY</b>.
<img width="50%" src = "img/CUA_b.png"/>
<br>
Podemos ver que hay evidencia de decrecimiento de O3 en estación: <b>CUA</b>.
<img width="50%" src = "img/HAN_b.png"/>
<br>
Podemos ver que hay evidencia de decrecimiento de PM10, NO2, O3 y NOX en estación: <b>HAN</b>.
<img width="50%" src = "img/IMP_b.png"/>
<br>
Podemos ver que hay evidencia de decrecimiento de CO en estación: <b>IMP</b>.
<img width="50%" src = "img/LAG_b.png"/>
<br>
Podemos ver que No hay evidencia en estación: <b>LAG</b>.
<img width="50%" src = "img/LLA_b.png"/>
<br>
Podemos ver que no hay evidencia en estación: <b>LLA</b>.
<img width="50%" src = "img/LVI_b.png"/>
<br>
Podemos ver que hay ligera eviencia en SO2 en estación: <b>LVI</b>.
<img width="50%" src = "img/MIN_b.png"/>
<br>
Podemos ver que no hay evidencia en estación: <b>MIN</b>.
<img width="50%" src = "img/PED_b.png"/>
<br>
Podemos ver que hay ligera evidencia en NO y NO2 en estación: <b>PED</b>.
<img width="50%" src = "img/PER_b.png"/>
<br>
Podemos ver que hay ligera evidencia en PM2.5 en estación: <b>PER</b>.
<img width="50%" src = "img/PLA_b.png"/>
<br>
Podemos ver que hay ligera evidencia en NOX en estación: <b>PLA</b>.
<img width="50%" src = "img/SAG_b.png"/>
<br>
Podemos ver que hay ligera evidencia en NOX en estación: <b>SAG</b>.
<img width="50%" src = "img/SJA_b.png"/>
<br>
Podemos ver que hay clara evidencia en PM2.5 en estación: <b>SJA</b>.
<img width="50%" src = "img/SUR_b.png"/>
<br>
Podemos ver que no hay evidencia en estación: <b>SUR</b>.
<img width="50%" src = "img/TAC_b.png"/>
<br>
Podemos ver que hay ligera evidencia en todas excepto CO en estación: <b>TAC</b>.
<img width="50%" src = "img/TAH_b.png"/>
<br>
Podemos ver que hay ligera evidencia en O3 en estación: <b>TAH</b>.

<img width="50%" src = "img/TAX_b.png"/>
<br>
Podemos ver que no hay evidencia en estación: <b>TAX</b>.

<img width="50%" src = "img/TLA_b.png"/>
<br>
Podemos ver que No hay evidencia en estación: <b>TLA</b>.

<img width="50%" src = "img/TLI_b.png"/>
<br>
Podemos ver que hay ligera evidencia en SO2 y PM10 en estación: <b>TLI</b>.

<img width="50%" src = "img/TPN_b.png"/>
<br>
Podemos ver que NO hay  evidencia en estación: <b>TPN</b>.

<img width="50%" src = "img/VAL_b.png"/>
<br>
Podemos ver que hay ligera evidencia en SO2 en estación: <b>VAL</b>.

<img width="50%" src = "img/VIF_b.png"/>
<br>
Podemos ver que hay ligera evidencia en PM10 en estación: <b>VIF</b>.

### Conclusión: 
Se sugiere un estudio más profundo en donde se encontró evidencia de que fué el metrobús. Lo primero sería descartar las estaciones que no son geográficamente relevantes. Después sería revisar por meses dónde fueron los cambios para comparar con aperturas de diferentes partes del metrobús. En principio parece que podría haber influencia del metrobús en algunas métricas para algunos parámetros.<br>
<br>
** El código para este ejercicio puede verse en <b>pregunta4/script_spark_2.py</b>.

### D.3 ¿Cuál es el contaminante que ha tenido mayor descenso con el paso del tiempo? Obtén el promedio por métrica del día (todas las estaciones), luego genera un lag por día, luego saca el promedio del lag por contaminante, selecciona el que tenga mayor lag.  →→  ordena de 1986 a 2017!

El contaminante con mayor descenso en el paso del tiempo es <b>NOX</b>:
<br>
<img width="50%" src="img/pregunta_c.png"/>
<br>
<br>
** El código para este ejercicio puede verse en <b>pregunta4/script_spark_3.py</b>.

### D.4 De acuerdo al histórico de PM2.5 ¿cuál es el mes del año en el que se pone peor?

<img width="80%" src="img/pregunta_d.png"/>

De acuerdo a su histórico, las temporadas del año cuando se pone peor son <b>Mayo</b> y <b>Diciembre/Enero</b>. Por un lado Diciembre es fácil de explicar por la alta cantidad de tráfico generada por consumo de fin de año, pero en Mayo es una interrogante que habría que investigar más a fondo. 
<br>
<br>
** El código para este ejercicio puede verse en <b>pregunta4/script_spark_4.py</b>.

### D.5 ¿En qué horario/mes evitarías irte en Ecobici a cualquier lugar de acuerdo al histórico de mediciones de PM2.5?

<img width = "50%" src="img/pregunta_e_grafica.png"/>

Definitivamente y sin lugar a futura discusión, a mediodía, esto es; entre 11:00 y 12:00 de la mañana, cualquier mes del año y cualquier horario entre más alejado de éste mejor. Los meses nos hablan de que Invierno es lo peor y verano-otoño lo menos grave.
<br>
<br>
** El código para este ejercicio puede verse en <b>pregunta4/script_spark_5.py</b>.


```python

```