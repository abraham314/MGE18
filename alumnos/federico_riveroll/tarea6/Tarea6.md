
# Métodos de Gran Escala
## Tarea 6
### Federico Riveroll Gonzalez

Para ésta tarea ligué pyspark con Jupyter Notebook para poder contestar aquí.<br><br>
<b>*Nota:</b> En esta tarea decidí resolver cada ejercicio con una sola línea de código, pero quiero aclarar que no es la mejor opción ni para el entendimiento ni para la memoria, sin embargo, la vida es muy corta para no usar las facilidades para recursión de Python y especialmente Pyspark.<br>


```python
from pyspark.sql import SQLContext
from pyspark.sql.types import *
```

### Ejercicio 1


```python
sqlContext = SQLContext(sc)

# Cargar los datos a spark
empleados = sqlContext.read.load('employees.csv', 
                      format='com.databricks.spark.csv', 
                      header='true', 
                      inferSchema='true')

# empleados.show()

ordenes = sqlContext.read.load('orders.csv', 
                      format='com.databricks.spark.csv', 
                      header='true', 
                      inferSchema='true')

# ordenes.show()
```

a. ¿Cuántos "jefes" hay en la tabla empleados? ¿Cuáles son estos jefes: número de empleado, nombre, apellido, título, fecha de nacimiento, fecha en que iniciaron en la empresa, ciudad y país? (atributo reportsto, ocupa explode en tu respuesta)


```python
empleados.select('employeeid','lastname','firstname','title').join(empleados.select('reportsto').distinct(), empleados.select('reportsto').distinct().reportsto == empleados.employeeid).show()
```

    +----------+--------+---------+--------------------+---------+
    |employeeid|lastname|firstname|               title|reportsto|
    +----------+--------+---------+--------------------+---------+
    |         2|  Fuller|   Andrew|Vice President, S...|        2|
    |         5|Buchanan|   Steven|       Sales Manager|        5|
    +----------+--------+---------+--------------------+---------+
    


b. ¿Quién es el segundo "mejor" empleado que más órdenes ha generado? (nombre, apellido, título, cuándo entró a la compañía, número de órdenes generadas, número de órdenes generadas por el mejor empleado (número 1))


```python
ordenes.groupBy('employeeid').count().orderBy('count', ascending=False).join(empleados.select('employeeid', 'lastname', 'firstname', 'title', 'hiredate'), empleados.employeeid == ordenes.employeeid).select('lastname', 'firstname', 'title', 'hiredate', 'count').show(2)
```

    +---------+---------+--------------------+-------------------+-----+
    | lastname|firstname|               title|           hiredate|count|
    +---------+---------+--------------------+-------------------+-----+
    |  Peacock| Margaret|Sales Representative|1993-05-03 00:00:00|  156|
    |Leverling|    Janet|Sales Representative|1992-04-01 00:00:00|  127|
    +---------+---------+--------------------+-------------------+-----+
    only showing top 2 rows
    


c. ¿Cuál es el delta de tiempo más grande entre una orden y otra?


```python
max([[ordenes.select('orderdate').rdd.flatMap(list).collect()[i + 1] - ordenes.select('orderdate').rdd.flatMap(list).collect()[i]] for i in range(len(ordenes.select('orderdate').rdd.flatMap(list).collect())-1)])
```




    [datetime.timedelta(3)]



### Ejercicio 2


```python
# Cargar los datos a spark

airports = sqlContext.read.load('airports.csv', 
                      format='com.databricks.spark.csv', 
                      header='true', 
                      inferSchema='true')

#airports.show()
```


```python
airlines = sqlContext.read.load('airlines.csv', 
                      format='com.databricks.spark.csv', 
                      header='true', 
                      inferSchema='true')

# airlines.show()
```


```python
flights = sqlContext.read.load('flights.csv', 
                      format='com.databricks.spark.csv', 
                      header='true', 
                      inferSchema='true')

# flights.show()
```

### a. ¿Qué aerolíneas (nombres) llegan al aeropuerto "Honolulu International Airport"?


```python
flights.select('AIRLINE', 'DESTINATION_AIRPORT').join(airlines, airlines.IATA_CODE == flights.AIRLINE).join(airports, flights.DESTINATION_AIRPORT == airports.IATA_CODE).filter(airports.AIRPORT == "Honolulu International Airport").select(airlines['AIRLINE']).distinct().show()
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
    


### b. ¿En qué horario (hora del día, no importan los minutos) hay salidas del aeropuerto de San Francisco ("SFO") a "Honolulu International Airport"?


```python
flights.select('SCHEDULED_DEPARTURE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT').filter(flights.ORIGIN_AIRPORT == 'SFO').join(airports, airports.IATA_CODE == flights.DESTINATION_AIRPORT).filter(airports.AIRPORT == 'Honolulu International Airport').select('SCHEDULED_DEPARTURE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT').distinct().show()
```

    +-------------------+--------------+-------------------+
    |SCHEDULED_DEPARTURE|ORIGIN_AIRPORT|DESTINATION_AIRPORT|
    +-------------------+--------------+-------------------+
    |               1609|           SFO|                HNL|
    |               1909|           SFO|                HNL|
    |                720|           SFO|                HNL|
    |                855|           SFO|                HNL|
    |               1317|           SFO|                HNL|
    |               1605|           SFO|                HNL|
    |               1915|           SFO|                HNL|
    |               1854|           SFO|                HNL|
    |               1505|           SFO|                HNL|
    |               1913|           SFO|                HNL|
    |               1927|           SFO|                HNL|
    |               1500|           SFO|                HNL|
    |               1616|           SFO|                HNL|
    |               1910|           SFO|                HNL|
    |               1924|           SFO|                HNL|
    |               1930|           SFO|                HNL|
    |               1947|           SFO|                HNL|
    |               1310|           SFO|                HNL|
    |               1750|           SFO|                HNL|
    |                905|           SFO|                HNL|
    +-------------------+--------------+-------------------+
    only showing top 20 rows
    


### c. ¿Qué día de la semana y en qué aerolínea nos conviene viajar a "Honolulu International Airport" para tener el menor retraso posible? 


```python
flights.select('DEPARTURE_DELAY', 'DESTINATION_AIRPORT', 'DAY_OF_WEEK', 'AIRLINE').join(airports, airports.IATA_CODE == flights.DESTINATION_AIRPORT).filter(airports.AIRPORT == 'Honolulu International Airport').select('DEPARTURE_DELAY', 'DESTINATION_AIRPORT', 'DAY_OF_WEEK', 'AIRLINE').join(airlines, airlines.IATA_CODE == flights['AIRLINE']).orderBy('DEPARTURE_DELAY').show()
```

    +---------------+-------------------+-----------+-------+---------+--------------------+
    |DEPARTURE_DELAY|DESTINATION_AIRPORT|DAY_OF_WEEK|AIRLINE|IATA_CODE|             AIRLINE|
    +---------------+-------------------+-----------+-------+---------+--------------------+
    |           null|                HNL|          1|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          4|     UA|       UA|United Air Lines ...|
    |           null|                HNL|          3|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          1|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          2|     UA|       UA|United Air Lines ...|
    |           null|                HNL|          2|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          2|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          3|     UA|       UA|United Air Lines ...|
    |           null|                HNL|          2|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          7|     US|       US|     US Airways Inc.|
    |           null|                HNL|          2|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          3|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          3|     UA|       UA|United Air Lines ...|
    |           null|                HNL|          1|     US|       US|     US Airways Inc.|
    |           null|                HNL|          3|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          7|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          3|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          1|     UA|       UA|United Air Lines ...|
    |           null|                HNL|          2|     HA|       HA|Hawaiian Airlines...|
    |           null|                HNL|          7|     HA|       HA|Hawaiian Airlines...|
    +---------------+-------------------+-----------+-------+---------+--------------------+
    only showing top 20 rows
    


### d. ¿Cuál es el aeropuerto con mayor tráfico de entrada? 


```python
flights.select('DESTINATION_AIRPORT').join(airports, airports.IATA_CODE == flights.DESTINATION_AIRPORT).groupBy('DESTINATION_AIRPORT').count().orderBy('count', ascending=False).join(airports, airports.IATA_CODE == flights['DESTINATION_AIRPORT']).select('count','AIRPORT').show()
```

    +------+--------------------+
    | count|             AIRPORT|
    +------+--------------------+
    |346904|Hartsfield-Jackso...|
    |285906|Chicago O'Hare In...|
    |239582|Dallas/Fort Worth...|
    |196010|Denver Internatio...|
    |194696|Los Angeles Inter...|
    |147966|San Francisco Int...|
    |146812|Phoenix Sky Harbo...|
    |146683|George Bush Inter...|
    |133198|McCarran Internat...|
    |112128|Minneapolis-Saint...|
    |110980|Orlando Internati...|
    |110907|Seattle-Tacoma In...|
    |108398|Detroit Metropoli...|
    |107851|Gen. Edward Lawre...|
    |101830|Newark Liberty In...|
    |100322|Charlotte Douglas...|
    | 99581|LaGuardia Airport...|
    | 97193|Salt Lake City In...|
    | 93809|John F. Kennedy I...|
    | 86085|Baltimore-Washing...|
    +------+--------------------+
    only showing top 20 rows
    


### e. ¿Cuál es la aerolínea con mayor retraso de salida por día de la semana?
Listas por día ordenados de 1 a 7.


```python
[[flights.select('AIRLINE', 'DEPARTURE_DELAY', 'DAY_OF_WEEK').filter(flights.DAY_OF_WEEK == dia).groupBy('AIRLINE').sum('DEPARTURE_DELAY').orderBy('sum(DEPARTURE_DELAY)', ascending=False).show(1)] for dia in range(1,7)]
```

    +-------+--------------------+
    |AIRLINE|sum(DEPARTURE_DELAY)|
    +-------+--------------------+
    |     WN|             2058198|
    +-------+--------------------+
    only showing top 1 row
    
    +-------+--------------------+
    |AIRLINE|sum(DEPARTURE_DELAY)|
    +-------+--------------------+
    |     WN|             1927701|
    +-------+--------------------+
    only showing top 1 row
    
    +-------+--------------------+
    |AIRLINE|sum(DEPARTURE_DELAY)|
    +-------+--------------------+
    |     WN|             1821657|
    +-------+--------------------+
    only showing top 1 row
    
    +-------+--------------------+
    |AIRLINE|sum(DEPARTURE_DELAY)|
    +-------+--------------------+
    |     WN|             2127931|
    +-------+--------------------+
    only showing top 1 row
    
    +-------+--------------------+
    |AIRLINE|sum(DEPARTURE_DELAY)|
    +-------+--------------------+
    |     WN|             2053053|
    +-------+--------------------+
    only showing top 1 row
    
    +-------+--------------------+
    |AIRLINE|sum(DEPARTURE_DELAY)|
    +-------+--------------------+
    |     WN|             1412164|
    +-------+--------------------+
    only showing top 1 row
    





    [[None], [None], [None], [None], [None], [None]]



### f. ¿Cuál es la tercer aerolínea con menor retraso de salida los lunes (day of week = 2)?


```python
flights.filter(flights.DAY_OF_WEEK == 2).select('AIRLINE', 'DEPARTURE_DELAY').groupBy('AIRLINE').sum('DEPARTURE_DELAY').orderBy('sum(DEPARTURE_DELAY)', ascending=False).show(3)
```

    +-------+--------------------+
    |AIRLINE|sum(DEPARTURE_DELAY)|
    +-------+--------------------+
    |     WN|             1927701|
    |     UA|             1121781|
    |     DL|              977001|
    +-------+--------------------+
    only showing top 3 rows
    


### g. ¿Cuál es el aeropuerto origen que llega a la mayor cantidad de aeropuertos destino diferentes?
Esta merece el premio nobel de las consultas en una línea :)


```python
max([[aeropuerto, flights.filter(flights.ORIGIN_AIRPORT == aeropuerto).select('DESTINATION_AIRPORT').distinct().count()] for aeropuerto in flights.select('ORIGIN_AIRPORT').distinct().rdd.flatMap(list).collect()], key=lambda x:x[1])
```




    ['ATL', 169]


