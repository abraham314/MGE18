
#### NOTAS:
##### Para cada inciso se reutilizo la base "aux" y "aux2" para no tener muchas bases en la memoria
##### la mayoria de los pasos los realice en lineas separadas por simplicidad en la construccion, aunque se que se pueden concatenar en muchas menos lineas
##### Agradezco a Daniel Sharp por mostrarme esta imagen de Docker  


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.master("local").appName("Tarea6").getOrCreate()
```


```python
# Se cargan las bases de datos que se van a utilizar en este ejercicio
employees = spark.read.csv('/home/jovyan/employees.csv', header =True)
orders = spark.read.csv('/home/jovyan/orders.csv', header =True)
```

### EJERCICIO 1

#### a)


```python
# Se imprimien cuantos jefes distintos hay. NOTA: por simplicidad no se guarda esto en un csv output, 
# se guarda la siguiente base.
aux = employees.select("reportsto")
aux = aux.filter(aux.reportsto != 'null').distinct()
aux.count()
```




    2




```python
# Se seleccionan las columnas de interes y filtran empleados que no reportan a nadie
aux = employees.select("reportsto","employeeid").filter(employees["reportsto"] != 'null')
# Se crea una columna en la que se tiene el conjunto de ids por reportsto
aux = aux.groupBy("reportsto").agg(collect_set("employeeid").alias("trabajadores"))
# Join de empleados con aux anterior
aux2 = employees.select("employeeid","firstname","lastname","title","birthdate","hiredate","city","country")
aux2 = aux2.join(aux, aux2.employeeid == aux.reportsto)
# Se usa explode
aux2 = aux2.select("employeeid","firstname","lastname","title","birthdate","hiredate","city","country",explode("trabajadores"))
aux2 = aux2.withColumnRenamed("col","trabajadores_ids")
aux2.repartition(1).write.csv("/home/jovyan/outputs/ejercicio1_a.csv", header = True)
aux2.show()
```

    +----------+---------+--------+--------------------+----------+----------+------+-------+----------------+
    |employeeid|firstname|lastname|               title| birthdate|  hiredate|  city|country|trabajadores_ids|
    +----------+---------+--------+--------------------+----------+----------+------+-------+----------------+
    |         2|   Andrew|  Fuller|Vice President, S...|1952-02-19|1992-08-14|Tacoma|    USA|               3|
    |         2|   Andrew|  Fuller|Vice President, S...|1952-02-19|1992-08-14|Tacoma|    USA|               1|
    |         2|   Andrew|  Fuller|Vice President, S...|1952-02-19|1992-08-14|Tacoma|    USA|               5|
    |         2|   Andrew|  Fuller|Vice President, S...|1952-02-19|1992-08-14|Tacoma|    USA|               8|
    |         2|   Andrew|  Fuller|Vice President, S...|1952-02-19|1992-08-14|Tacoma|    USA|               4|
    |         5|   Steven|Buchanan|       Sales Manager|1955-03-04|1993-10-17|London|     UK|               9|
    |         5|   Steven|Buchanan|       Sales Manager|1955-03-04|1993-10-17|London|     UK|               7|
    |         5|   Steven|Buchanan|       Sales Manager|1955-03-04|1993-10-17|London|     UK|               6|
    +----------+---------+--------+--------------------+----------+----------+------+-------+----------------+
    


#### b)


```python
# Se crea tabla con cantidad de ordenes por empleado
aux = orders.groupBy("employeeid").count()
# Se hace join de tabla anterior con tabla de empleados y se ordena
aux = aux.join(employees.select('employeeid', 'lastname', 'firstname', 'title', 'hiredate'), ["employeeid"]).orderBy("count",ascending=False)
# Se crea columna con maximo de ordenes
aux = aux.withColumn("maximo", lit(aux.agg({"count": "max"}).collect()[0][0]))
# Se elimina el empleado que tuvo mas ordenes
aux = aux.filter(aux["count"]<aux["maximo"])
# Se extra el segundo empleado con mas ordenes
aux = aux.limit(1)
# Se guardan resultados
aux.repartition(1).write.csv("/home/jovyan/outputs/ejercicio1_b.csv", header = True)
aux.show()
```

    +----------+-----+---------+---------+--------------------+----------+------+
    |employeeid|count| lastname|firstname|               title|  hiredate|maximo|
    +----------+-----+---------+---------+--------------------+----------+------+
    |         3|  127|Leverling|    Janet|Sales Representative|1992-04-01|   156|
    +----------+-----+---------+---------+--------------------+----------+------+
    


#### c)


```python
# Se ordena por orderId (por si no esta ordenada)
aux = orders.orderBy("orderId")
# Se crea tabla con fechas y lag de fecha
aux = aux.select("orderid","orderdate", lag("orderdate").over(Window.orderBy("orderid")).alias("lag"))
# Se crea columna con diferencia y se ordena de mayor a menor
aux = aux.withColumn("delta",datediff(to_date("orderdate"),to_date("lag"))).orderBy("delta",ascending=False)
# Se elije la primer observacion. NOTA: solo inclui el delta, porque eso es lo que se pide, para ver el
# resto de la informacion basta con quitar el select.
aux = aux.select("delta").limit(1)
aux.repartition(1).write.csv("/home/jovyan/outputs/ejercicio1_c.csv", header = True)
aux.show()
```

    +-----+
    |delta|
    +-----+
    |    3|
    +-----+
    


### EJERCICIO 2


```python
# Se cargan las bases de datos que se van a utilizar en este ejercicio
airlines = spark.read.csv('/home/jovyan/airlines.csv', header =True)
airports = spark.read.csv('/home/jovyan/airports.csv', header =True)
flights = spark.read.csv('/home/jovyan/flights.csv', header =True)
```

#### a)


```python
# Se seleccionan columnas relevantes
aux = flights.select("AIRLINE","DESTINATION_AIRPORT")
# Se hacen joins con otras bases
aux = aux.join(airlines, aux.AIRLINE == airlines.IATA_CODE)
aux = aux.join(airports, aux.DESTINATION_AIRPORT == airports.IATA_CODE)
# Se filtran registros con destino a Honolulu y se crea vector con aerolineas
aux = aux.filter(aux["AIRPORT"] == "Honolulu International Airport")
aux = aux.select(airlines['AIRLINE']).distinct()
aux.repartition(1).write.csv("/home/jovyan/outputs/ejercicio2_a.csv", header = True)
aux.show()
```

    +--------------------+
    |             AIRLINE|
    +--------------------+
    |      Virgin America|
    |United Air Lines ...|
    |     US Airways Inc.|
    |Hawaiian Airlines...|
    |Alaska Airlines Inc.|
    |Delta Air Lines Inc.|
    |American Airlines...|
    +--------------------+
    


#### b)


```python
# Se seleccionan columnas relevantes
aux = flights.select("SCHEDULED_DEPARTURE","ORIGIN_AIRPORT","DESTINATION_AIRPORT")
# Se hace join con airports
aux = aux.join(airports, aux.DESTINATION_AIRPORT == airports.IATA_CODE)
# Como en la pregunta se especifica "SFO", no se hace join con origin airports
aux = aux.filter((aux["ORIGIN_AIRPORT"] == "SFO") & (aux["AIRPORT"] == "Honolulu International Airport"))
# Se selecciona la columna de interes, solo se utilizan los primeros dos digitos.
aux = aux.select(substring("SCHEDULED_DEPARTURE",1,2).alias("HORA_SALIDA")).distinct().orderBy("HORA_SALIDA")
aux.repartition(1).write.csv("/home/jovyan/outputs/ejercicio2_b.csv", header = True)
aux.show()
```

    +-----------+
    |HORA_SALIDA|
    +-----------+
    |         06|
    |         07|
    |         08|
    |         09|
    |         10|
    |         11|
    |         12|
    |         13|
    |         14|
    |         15|
    |         16|
    |         17|
    |         18|
    |         19|
    +-----------+
    


#### c)


```python
# Se seleccionan columnas relevantes
aux = flights.select("AIRLINE","DESTINATION_AIRPORT","ARRIVAL_DELAY","DAY_OF_WEEK")
# Se hace join con base airports y se filtran los de interes
aux = aux.join(airports, aux.DESTINATION_AIRPORT == airports.IATA_CODE)
aux = aux.filter((aux["AIRPORT"] == "Honolulu International Airport"))
# Se calcula retraso de arrivo promedio
aux = aux.groupBy("AIRLINE","DAY_OF_WEEK").agg(mean("ARRIVAL_DELAY").alias("DELAY")).orderBy("DELAY").limit(1)
# SE hace merge para tener nombre de aerolinea
aux = aux.withColumnRenamed("AIRLINE","IATA_CODE")
aux = aux.join(airlines, ["IATA_CODE"])
aux.repartition(1).write.csv("/home/jovyan/outputs/ejercicio2_c.csv", header = True)
aux.show()
```

    +---------+-----------+-------------------+--------------+
    |IATA_CODE|DAY_OF_WEEK|              DELAY|       AIRLINE|
    +---------+-----------+-------------------+--------------+
    |       VX|          1|-14.222222222222221|Virgin America|
    +---------+-----------+-------------------+--------------+
    


#### d)


```python
# Se cuentan vuelos distintos a cada aeropuerto, se selecciona el mayor
aux = flights.groupBy("DESTINATION_AIRPORT").count().orderBy("count",ascending=False).limit(1)
# Join con base de airports
aux = aux.join(airports, aux.DESTINATION_AIRPORT == airports.IATA_CODE).select("AIRPORT","count")
aux.repartition(1).write.csv("/home/jovyan/outputs/ejercicio2_d.csv", header = True)
aux.show()
```

    +--------------------+------+
    |             AIRPORT| count|
    +--------------------+------+
    |Hartsfield-Jackso...|346904|
    +--------------------+------+
    


#### e)


```python
# Se seleccionan columnas relevantes
aux = flights.select("AIRLINE","DEPARTURE_DELAY","DAY_OF_WEEK")
# Se calcula para cada dia y aerolinea el retraso promedio
aux = aux.groupBy("AIRLINE","DAY_OF_WEEK").agg(mean("DEPARTURE_DELAY").alias("DELAY"))
# Se guarda en otra base el maximo retraso por dia
aux2 = aux.groupBy("DAY_OF_WEEK").agg(max("DELAY").alias("MAX_DELAY"))
aux2 = aux2.withColumnRenamed("DAY_OF_WEEK","DAY")
# Se hace join de base de maximos con base aux para obtener la aerolinea con mayor retraso
aux2 = aux2.join(aux, (aux.DELAY == aux2.MAX_DELAY) & (aux.DAY_OF_WEEK == aux2.DAY))
aux2 = aux2.withColumnRenamed("AIRLINE","IATA_CODE")
# Join con airlines para tener nombre de aerolinea
aux2 = aux2.join(airlines, ["IATA_CODE"])
aux2 = aux2.select("DAY","AIRLINE","DELAY").orderBy("DAY")
aux2.repartition(1).write.csv("/home/jovyan/outputs/ejercicio2_e.csv", header = True)
aux2.show()
```

    +---+--------------------+------------------+
    |DAY|             AIRLINE|             DELAY|
    +---+--------------------+------------------+
    |  1|    Spirit Air Lines|18.795626679697044|
    |  2|    Spirit Air Lines|15.714659197012137|
    |  3|United Air Lines ...|14.282200834726739|
    |  4|United Air Lines ...| 14.88742760925581|
    |  5|    Spirit Air Lines|15.786104837751099|
    |  6|    Spirit Air Lines|15.780295269659536|
    |  7|    Spirit Air Lines|17.149202320522118|
    +---+--------------------+------------------+
    


#### f)


```python
# Se seleccionan columnas relevantes, se filtran los de dia igual a 2
aux = flights.select("AIRLINE","DEPARTURE_DELAY","DAY_OF_WEEK").filter(flights["DAY_OF_WEEK"] == 2)
# Se calcula delay promedio por aerolinea
aux = aux.groupBy("AIRLINE").agg(mean("DEPARTURE_DELAY").alias("DELAY")).orderBy("DELAY",ascending=True)
# Se calcula minimo y se elimina la menor. Se hace esto dos veces. NOTA: esto no es la mejor forma
# pero funciona
aux = aux.withColumn("minimo", lit(aux.agg({"DELAY": "min"}).collect()[0][0]))
aux = aux.filter(aux["DELAY"]>aux["MINIMO"])
aux = aux.withColumn("minimo", lit(aux.agg({"DELAY": "min"}).collect()[0][0]))
aux = aux.filter(aux["DELAY"]>aux["MINIMO"]).limit(1)
# Join con airlines para tener nombre
aux = aux.withColumnRenamed("AIRLINE","IATA_CODE")
aux = aux.join(airlines,["IATA_CODE"]).select("AIRLINE","DELAY")
aux.repartition(1).write.csv("/home/jovyan/outputs/ejercicio2_f.csv", header = True)
aux.show()
```

    +---------------+-----------------+
    |        AIRLINE|            DELAY|
    +---------------+-----------------+
    |US Airways Inc.|6.475248598806726|
    +---------------+-----------------+
    


#### g)


```python
aux = flights.select("ORIGIN_AIRPORT","DESTINATION_AIRPORT")
aux = aux.join(airports, aux.ORIGIN_AIRPORT == airports.IATA_CODE)
aux = aux.groupBy("AIRPORT").agg(countDistinct("DESTINATION_AIRPORT").alias("count")).orderBy("count",ascending=False).limit(1)
aux.repartition(1).write.csv("/home/jovyan/outputs/ejercicio2_g.csv", header = True)
aux.show()
```

    +--------------------+-----+
    |             AIRPORT|count|
    +--------------------+-----+
    |Hartsfield-Jackso...|  169|
    +--------------------+-----+
    



```python
spark.stop()
```
