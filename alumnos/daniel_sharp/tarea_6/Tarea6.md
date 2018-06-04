
# Daniel Sharp 138176
## Tarea 6 - Spark

Iniciamos la Spark Session


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").getOrCreate()
```

Carga de las tablas a Spark


```python
employees = spark.read.csv('/home/jovyan/northwind/employees.csv', header =True)
orders = spark.read.csv('/home/jovyan/northwind/orders.csv', header =True)
customers = spark.read.csv('/home/jovyan/northwind/customers.csv', header =True)
order_details = spark.read.csv('/home/jovyan/northwind/order_details.csv', header =True)
products = spark.read.csv('/home/jovyan/northwind/products.csv', header =True)
```

### Ejercicio 1
#### a. ¿Cuántos “jefes” hay en la tabla empleados? ¿Cuáles son estos jefes: número de empleado, nombre, apellido, título, fecha de nacimiento, fecha en que iniciaron en la empresa, ciudad y país? (atributo reportsto, ocupa explode en tu respuesta) 


```python
ej1_a1=employees.agg(countDistinct("reportsto").alias("Numero de jefes"))
ej1_a1.show()
ej1_a1.repartition(1).write.csv("/home/jovyan/northwind/output/ej1_a1.csv", header = True)
```

    +---------------+
    |Numero de jefes|
    +---------------+
    |              2|
    +---------------+
    



```python
ej1_a2=employees.select("employeeid","firstname","lastname","title","birthdate", "hiredate","city","country").alias("t1")\
.join(employees.alias("t2").select("reportsto","firstname").where("reportsto > 0")\
.groupBy("reportsto").agg(collect_set("firstname").alias("subs")),col("t1.employeeid")==col("t2.reportsto"))\
.select("employeeid","firstname","lastname","title","birthdate", "hiredate","city","country",explode("subs"))
ej1_a2.show()
ej1_a2.repartition(1).write.csv("/home/jovyan/northwind/output/ej1_a2.csv", header = True)
```

    +----------+---------+--------+--------------------+----------+----------+------+-------+--------+
    |employeeid|firstname|lastname|               title| birthdate|  hiredate|  city|country|     col|
    +----------+---------+--------+--------------------+----------+----------+------+-------+--------+
    |         2|   Andrew|  Fuller|Vice President, S...|1952-02-19|1992-08-14|Tacoma|    USA|   Janet|
    |         2|   Andrew|  Fuller|Vice President, S...|1952-02-19|1992-08-14|Tacoma|    USA|   Laura|
    |         2|   Andrew|  Fuller|Vice President, S...|1952-02-19|1992-08-14|Tacoma|    USA|   Nancy|
    |         2|   Andrew|  Fuller|Vice President, S...|1952-02-19|1992-08-14|Tacoma|    USA|Margaret|
    |         2|   Andrew|  Fuller|Vice President, S...|1952-02-19|1992-08-14|Tacoma|    USA|  Steven|
    |         5|   Steven|Buchanan|       Sales Manager|1955-03-04|1993-10-17|London|     UK|  Robert|
    |         5|   Steven|Buchanan|       Sales Manager|1955-03-04|1993-10-17|London|     UK|    Anne|
    |         5|   Steven|Buchanan|       Sales Manager|1955-03-04|1993-10-17|London|     UK| Michael|
    +----------+---------+--------+--------------------+----------+----------+------+-------+--------+
    


#### b. ¿Quién es el segundo “mejor” empleado que más órdenes ha generado? (nombre, apellido, título, cuándo entró a la compañía, número de órdenes generadas, número de órdenes generadas por el mejor empleado (número 1))


```python
from pyspark.sql import Window
ej1_b=orders.alias("t1").select("employeeid").groupBy("employeeid").agg(count("*").alias("sales")).\
join(employees.alias("t2").select("employeeid","firstname","lastname","title","hiredate"),\
     ["employeeid"]).orderBy(desc("sales")).alias("t3")\
.select("*",lag("sales").over(Window.orderBy(desc("sales"))).alias("max_sales"))\
.select("*",row_number().over(Window.orderBy(desc("sales"))).alias("rn")).filter("rn == 2").drop("rn")
ej1_b.show()
ej1_b.repartition(1).write.csv("/home/jovyan/northwind/output/ej1_b.csv", header = True)
```

    +----------+-----+---------+---------+--------------------+----------+---------+
    |employeeid|sales|firstname| lastname|               title|  hiredate|max_sales|
    +----------+-----+---------+---------+--------------------+----------+---------+
    |         3|  127|    Janet|Leverling|Sales Representative|1992-04-01|      156|
    +----------+-----+---------+---------+--------------------+----------+---------+
    


#### c. ¿Cuál es el delta de tiempo más grande entre una orden y otra?


```python
ej1_c=orders.select("orderid",to_date("orderdate").alias("date"))\
.select("*",lag("date").over(Window.orderBy("orderid")).alias("lag"))\
.select("orderid", datediff("date","lag").alias("delta")).orderBy(desc("delta"))\
.limit(5)
ej1_c.show()
ej1_c.repartition(1).write.csv("/home/jovyan/northwind/output/ej1_c.csv", header = True)
```

    +-------+-----+
    |orderid|delta|
    +-------+-----+
    |  10262|    3|
    |  10284|    3|
    |  10267|    3|
    |  10256|    3|
    |  10273|    3|
    +-------+-----+
    


### Ejercicio 2

Carga de tablas a Spark


```python
flights = spark.read.csv('/home/jovyan/flights/flights.csv', header =True)
airports = spark.read.csv('/home/jovyan/flights/airports.csv', header =True)
airlines = spark.read.csv('/home/jovyan/flights/airlines.csv', header =True)
```

#### a. ¿Qué aerolíneas (nombres) llegan al aeropuerto “Honolulu International Airport”?


```python
ej2_a=flights.alias("f").select("*").withColumnRenamed("AIRLINE","AIRLINE_C")\
.join(airlines.alias("al"), col("AIRLINE_C")==col("al.IATA_CODE"))\
.join(airports.alias("ap"), col("f.DESTINATION_AIRPORT")==col("ap.IATA_CODE"))\
.select("al.AIRLINE").filter(col("AIRPORT").like('Honolulu International Airport')).distinct()
ej2_a.show()
ej2_a.repartition(1).write.csv("/home/jovyan/flights/output/ej2_a.csv", header = True)
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
    


#### b. ¿En qué horario (hora del día, no importan los minutos) hay salidas del aeropuerto de San Francisco (“SFO”) a “Honolulu International Airport”?


```python
ej2_b=flights.alias("f").select("SCHEDULED_DEPARTURE","DESTINATION_AIRPORT","ORIGIN_AIRPORT")\
.join(airports.alias("ap").withColumnRenamed("AIRPORT","DEST_AIRPORT"), col("f.DESTINATION_AIRPORT") == col("ap.IATA_CODE"))\
.join(airports.alias("ap2"), col("f.ORIGIN_AIRPORT") == col("ap2.IATA_CODE"))\
.select(substring("SCHEDULED_DEPARTURE",1,2).alias("hora")).filter((col("DEST_AIRPORT").like('Honolulu International Airport'))\
                                                    & (col("AIRPORT").like('San Francisco International Airport')))\
.distinct().orderBy("hora")
ej2_b.show()
ej2_b.repartition(1).write.csv("/home/jovyan/flights/output/ej2_b.csv", header = True)
```

    +----+
    |hora|
    +----+
    |  06|
    |  07|
    |  08|
    |  09|
    |  10|
    |  11|
    |  12|
    |  13|
    |  14|
    |  15|
    |  16|
    |  17|
    |  18|
    |  19|
    +----+
    


#### c. ¿Qué día de la semana y en qué aerolínea nos conviene viajar a “Honolulu International Airport” para tener el menor retraso posible?


```python
ej2_c=flights.alias("f").select("DAY_OF_WEEK","DEPARTURE_DELAY","DESTINATION_AIRPORT","AIRLINE")\
.withColumnRenamed("AIRLINE","AIRLINE_C")\
.join(airports.alias("ap").select("IATA_CODE","AIRPORT"), col("f.DESTINATION_AIRPORT")==col("ap.IATA_CODE"))\
.join(airlines.alias("al"), col("AIRLINE_C")==col("al.IATA_CODE"))\
.select("f.DAY_OF_WEEK","al.AIRLINE","f.DEPARTURE_DELAY","AIRPORT")\
.filter((col("AIRPORT").like('Honolulu International Airport'))).filter("f.DEPARTURE_DELAY >= 0")\
.groupBy("f.DAY_OF_WEEK","al.AIRLINE").agg(mean("f.DEPARTURE_DELAY").alias("avg_delay"))\
.orderBy("avg_delay").filter("avg_delay > 0").limit(5)
ej2_c.show()
ej2_c.repartition(1).write.csv("/home/jovyan/flights/output/ej2_c.csv", header = True)
```

    +-----------+--------------+------------------+
    |DAY_OF_WEEK|       AIRLINE|         avg_delay|
    +-----------+--------------+------------------+
    |          7|Virgin America|               3.2|
    |          1|Virgin America|3.4444444444444446|
    |          6|Virgin America|              3.75|
    |          2|Virgin America| 6.222222222222222|
    |          3|Virgin America|               6.5|
    +-----------+--------------+------------------+
    


#### d. ¿Cuál es el aeropuerto con mayor tráfico de entrada?


```python
ej2_d=flights.alias("f").select("DESTINATION_AIRPORT")\
.join(airports.alias("ap").select("AIRPORT","IATA_CODE"),col("f.DESTINATION_AIRPORT")==col("ap.IATA_CODE"))\
.select("ap.AIRPORT").groupBy("ap.AIRPORT").agg(count("*").alias("traffic"))\
.orderBy(desc("traffic")).limit(5)
ej2_d.show()
ej2_d.repartition(1).write.csv("/home/jovyan/flights/output/ej2_d.csv", header = True)
```

    +--------------------+-------+
    |             AIRPORT|traffic|
    +--------------------+-------+
    |Hartsfield-Jackso...| 346904|
    |Chicago O'Hare In...| 285906|
    |Dallas/Fort Worth...| 239582|
    |Denver Internatio...| 196010|
    |Los Angeles Inter...| 194696|
    +--------------------+-------+
    


#### e. ¿Cuál es la aerolínea con mayor retraso de salida por día de la semana?


```python
t1 = flights.alias("f").select("AIRLINE","DEPARTURE_DELAY","DAY_OF_WEEK").withColumnRenamed("AIRLINE","AIRLINE_C")\
.join(airlines.alias("al").select("AIRLINE","IATA_CODE"), col("AIRLINE_C")==col("al.IATA_CODE"))\
.select("f.DAY_OF_WEEK","al.AIRLINE","f.DEPARTURE_DELAY").filter("f.DEPARTURE_DELAY > 0")\
.groupBy("f.DAY_OF_WEEK","al.AIRLINE").agg(mean("f.DEPARTURE_DELAY").alias("mean_delay"))
```


```python
ej2_e=t1.alias("a").select("*").groupBy("DAY_OF_WEEK").agg(max("mean_delay").alias("max_delay"))\
.join(t1.alias("b"), col("b.mean_delay")==col("max_delay"),"inner")\
.select("AIRLINE","a.DAY_OF_WEEK","max_delay").orderBy("a.DAY_OF_WEEK")
ej2_e.show()
ej2_e.repartition(1).write.csv("/home/jovyan/flights/output/ej2_e.csv", header = True)
```

    +--------------------+-----------+------------------+
    |             AIRLINE|DAY_OF_WEEK|         max_delay|
    +--------------------+-----------+------------------+
    |Frontier Airlines...|          1| 50.21651871864145|
    |Frontier Airlines...|          2| 47.32356687898089|
    |Frontier Airlines...|          3|43.845513963161025|
    |Atlantic Southeas...|          4| 41.68386779237497|
    |Frontier Airlines...|          5| 41.87589683924762|
    |Frontier Airlines...|          6|44.572584171403584|
    |Frontier Airlines...|          7| 44.15995307000391|
    +--------------------+-----------+------------------+
    


#### f. ¿Cuál es la tercer aerolínea con menor retraso de salida los lunes (day of week = 2)?


```python
ej2_f=flights.alias("f").select("DAY_OF_WEEK","DEPARTURE_DELAY","AIRLINE").withColumnRenamed("AIRLINE","AIRLINE_C")\
.join(airlines.alias("al").select("AIRLINE","IATA_CODE"), col("AIRLINE_C")==col("al.IATA_CODE"))\
.select("f.DAY_OF_WEEK","al.AIRLINE","f.DEPARTURE_DELAY").filter("f.DEPARTURE_DELAY > 0").filter("DAY_OF_WEEK == 2")\
.groupBy("f.DAY_OF_WEEK","al.AIRLINE").agg(mean("f.DEPARTURE_DELAY").alias("mean_delay"))\
.orderBy("mean_delay").limit(5)
ej2_f.show()
ej2_f.repartition(1).write.csv("/home/jovyan/flights/output/ej2_f.csv", header = True)
```

    +-----------+--------------------+------------------+
    |DAY_OF_WEEK|             AIRLINE|        mean_delay|
    +-----------+--------------------+------------------+
    |          2|Hawaiian Airlines...|13.919187358916478|
    |          2|Alaska Airlines Inc.|24.793569017587494|
    |          2|Southwest Airline...| 27.68090909090909|
    |          2|      Virgin America|29.154826377827334|
    |          2|American Airlines...|31.113598774885144|
    +-----------+--------------------+------------------+
    


#### g. ¿Cuál es el aeropuerto origen que llega a la mayor cantidad de aeropuertos destino diferentes?


```python
ej2_g=flights.alias("f").select("ORIGIN_AIRPORT","DESTINATION_AIRPORT")\
.join(airports.alias("ap").select("AIRPORT","IATA_CODE"), col("f.ORIGIN_AIRPORT")==col("ap.IATA_CODE"))\
.groupBy("ap.AIRPORT").agg(countDistinct("f.DESTINATION_AIRPORT").alias("destinations"))\
.orderBy(desc("destinations")).limit(5)
ej2_g.show()
ej2_g.repartition(1).write.csv("/home/jovyan/flights/output/ej2_g.csv", header = True)
```

    +--------------------+------------+
    |             AIRPORT|destinations|
    +--------------------+------------+
    |Hartsfield-Jackso...|         169|
    |Chicago O'Hare In...|         162|
    |Dallas/Fort Worth...|         148|
    |Denver Internatio...|         139|
    |Minneapolis-Saint...|         120|
    +--------------------+------------+
    



```python
spark.stop()
```
