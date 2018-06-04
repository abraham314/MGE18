
#### A continuación inicializaremos una sesión de Spark y correremos los queries para responder las preguntas de la vez anterior. Para inicializar Spark:


```python
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
```

#### Cargamos los archivos


```python
df = spark.read.load('order_details.csv',
                     format="csv", sep=",", inferSchema="true", header="true")
```


```python
employees  = spark.read.load('employees.csv',
                     format="csv", sep=",", inferSchema="true", header="true")
employees.printSchema()

```

    root
     |-- employeeid: integer (nullable = true)
     |-- lastname: string (nullable = true)
     |-- firstname: string (nullable = true)
     |-- title: string (nullable = true)
     |-- titleofcourtesy: string (nullable = true)
     |-- birthdate: timestamp (nullable = true)
     |-- hiredate: timestamp (nullable = true)
     |-- address: string (nullable = true)
     |-- city: string (nullable = true)
     |-- region: string (nullable = true)
     |-- postalcode: string (nullable = true)
     |-- country: string (nullable = true)
     |-- homephone: string (nullable = true)
     |-- extension: integer (nullable = true)
     |-- photo: string (nullable = true)
     |-- notes: string (nullable = true)
     |-- reportsto: integer (nullable = true)
     |-- photopath: string (nullable = true)
    



```python
order_details = spark.read.load('order_details.csv',
                     format="csv", sep=",", inferSchema="true", header="true")
order_details.printSchema()
```

    root
     |-- orderid: integer (nullable = true)
     |-- productid: integer (nullable = true)
     |-- unitprice: double (nullable = true)
     |-- quantity: integer (nullable = true)
     |-- discount: double (nullable = true)
    



```python
orders = spark.read.load('orders.csv',
                     format="csv", sep=",", inferSchema="true", header="true")
orders.printSchema()
```

    root
     |-- orderid: integer (nullable = true)
     |-- customerid: string (nullable = true)
     |-- employeeid: integer (nullable = true)
     |-- orderdate: timestamp (nullable = true)
     |-- requireddate: timestamp (nullable = true)
     |-- shippeddate: timestamp (nullable = true)
     |-- shipvia: integer (nullable = true)
     |-- freight: double (nullable = true)
     |-- shipname: string (nullable = true)
     |-- shipaddress: string (nullable = true)
     |-- shipcity: string (nullable = true)
     |-- shipregion: string (nullable = true)
     |-- shippostalcode: string (nullable = true)
     |-- shipcountry: string (nullable = true)
    



```python
customers = spark.read.load('customers.csv',
                     format="csv", sep=",", inferSchema="true", header="true")
customers.printSchema()
```

    root
     |-- customerid: string (nullable = true)
     |-- companyname: string (nullable = true)
     |-- contactname: string (nullable = true)
     |-- contacttitle: string (nullable = true)
     |-- address: string (nullable = true)
     |-- city: string (nullable = true)
     |-- region: string (nullable = true)
     |-- postalcode: string (nullable = true)
     |-- country: string (nullable = true)
     |-- phone: string (nullable = true)
     |-- fax: string (nullable = true)
    



```python
products = spark.read.load('products.csv',
                     format="csv", sep=",", inferSchema="true", header="true")
products.printSchema()
```

    root
     |-- productid: integer (nullable = true)
     |-- productname: string (nullable = true)
     |-- supplierid: integer (nullable = true)
     |-- categoryid: integer (nullable = true)
     |-- quantityperunit: string (nullable = true)
     |-- unitprice: double (nullable = true)
     |-- unitsinstock: integer (nullable = true)
     |-- unitsonorder: integer (nullable = true)
     |-- reorderlevel: integer (nullable = true)
     |-- discontinued: integer (nullable = true)
    



```python
flights = spark.read.load('flights.csv',
                     format="csv", sep=",", inferSchema="true", header="true")
flights.printSchema()
```

    root
     |-- YEAR: integer (nullable = true)
     |-- MONTH: integer (nullable = true)
     |-- DAY: integer (nullable = true)
     |-- DAY_OF_WEEK: integer (nullable = true)
     |-- AIRLINE: string (nullable = true)
     |-- FLIGHT_NUMBER: integer (nullable = true)
     |-- TAIL_NUMBER: string (nullable = true)
     |-- ORIGIN_AIRPORT: string (nullable = true)
     |-- DESTINATION_AIRPORT: string (nullable = true)
     |-- SCHEDULED_DEPARTURE: integer (nullable = true)
     |-- DEPARTURE_TIME: integer (nullable = true)
     |-- DEPARTURE_DELAY: integer (nullable = true)
     |-- TAXI_OUT: integer (nullable = true)
     |-- WHEELS_OFF: integer (nullable = true)
     |-- SCHEDULED_TIME: integer (nullable = true)
     |-- ELAPSED_TIME: integer (nullable = true)
     |-- AIR_TIME: integer (nullable = true)
     |-- DISTANCE: integer (nullable = true)
     |-- WHEELS_ON: integer (nullable = true)
     |-- TAXI_IN: integer (nullable = true)
     |-- SCHEDULED_ARRIVAL: integer (nullable = true)
     |-- ARRIVAL_TIME: integer (nullable = true)
     |-- ARRIVAL_DELAY: integer (nullable = true)
     |-- DIVERTED: integer (nullable = true)
     |-- CANCELLED: integer (nullable = true)
     |-- CANCELLATION_REASON: string (nullable = true)
     |-- AIR_SYSTEM_DELAY: integer (nullable = true)
     |-- SECURITY_DELAY: integer (nullable = true)
     |-- AIRLINE_DELAY: integer (nullable = true)
     |-- LATE_AIRCRAFT_DELAY: integer (nullable = true)
     |-- WEATHER_DELAY: integer (nullable = true)
    



```python
airports = spark.read.load('airports.csv',
                     format="csv", sep=",", inferSchema="true", header="true")
airports.printSchema()
```

    root
     |-- IATA_CODE: string (nullable = true)
     |-- AIRPORT: string (nullable = true)
     |-- CITY: string (nullable = true)
     |-- STATE: string (nullable = true)
     |-- COUNTRY: string (nullable = true)
     |-- LATITUDE: double (nullable = true)
     |-- LONGITUDE: double (nullable = true)
    



```python
airlines = spark.read.load('airlines.csv',
                     format="csv", sep=",", inferSchema="true", header="true")
airlines.printSchema()
```

    root
     |-- IATA_CODE: string (nullable = true)
     |-- AIRLINE: string (nullable = true)
    


#### a. ¿Cuántos "jefes" hay en la tabla empleados? ¿Cuáles son estos jefes: número de empleado, nombre, apellido, título, fecha de nacimiento, fecha en que iniciaron en la empresa, ciudad y país?


```python
employees.createOrReplaceTempView("employees")
p_1 = spark.sql("SELECT employeeid,firstname,lastname,titleofcourtesy,birthdate,hiredate,city,country FROM employees where employeeid IN ( select distinct reportsto from employees where reportsto >0)")
p_1.show()
```

    +----------+---------+--------+---------------+-------------------+-------------------+------+-------+
    |employeeid|firstname|lastname|titleofcourtesy|          birthdate|           hiredate|  city|country|
    +----------+---------+--------+---------------+-------------------+-------------------+------+-------+
    |         2|   Andrew|  Fuller|            Dr.|1952-02-19 00:00:00|1992-08-14 00:00:00|Tacoma|    USA|
    |         5|   Steven|Buchanan|            Mr.|1955-03-04 00:00:00|1993-10-17 00:00:00|London|     UK|
    +----------+---------+--------+---------------+-------------------+-------------------+------+-------+
    



```python
p1=p_1.collect()
#p_1.write.save("query1.csv",format="csv")
p1s = pd.DataFrame(p1)
p1s.to_csv("salida1.csv", sep=',',index=False)
```

#### b. ¿Quién es el segundo "mejor" empleado que más órdenes ha generado? (nombre, apellido, título, cuándo entró a la compañía, número de órdenes generadas, número de órdenes generadas por el mejor empleado (número 1))


```python
orders.createOrReplaceTempView("orders")
p_2 = spark.sql("SELECT lastname, firstname, title, hiredate, count(*) as total_ordenes FROM orders JOIN employees ON orders.employeeid = employees.employeeid GROUP BY lastname, firstname, title, hiredate ORDER BY total_ordenes DESC LIMIT 2")
p_2.show()
```

    +---------+---------+--------------------+-------------------+-------------+
    | lastname|firstname|               title|           hiredate|total_ordenes|
    +---------+---------+--------------------+-------------------+-------------+
    |  Peacock| Margaret|Sales Representative|1993-05-03 00:00:00|          156|
    |Leverling|    Janet|Sales Representative|1992-04-01 00:00:00|          127|
    +---------+---------+--------------------+-------------------+-------------+
    



```python
p2=p_2.collect()
#p_2.write.save("query2.csv",format="csv")
p2s = pd.DataFrame(p2)
p2s.to_csv("salida2.csv", sep=',',index=False)
```

 #### c. ¿Cuál es el delta de tiempo más grande entre una orden y otra?


```python
p_3=spark.sql("SELECT orderid, from_utc_timestamp(date_format(orderdate,'yyyy-MM-dd HH:mm:ss.SSS'),'UTC') orderdate, datediff(orderdate,from_utc_timestamp(date_format(lag(orderdate) OVER(ORDER BY orderid),'yyyy-MM-dd HH:mm:ss.SSS'),'UTC')) delta FROM orders ORDER BY delta DESC LIMIT 5")
p_3.show()
```

    +-------+-------------------+-----+
    |orderid|          orderdate|delta|
    +-------+-------------------+-----+
    |  10262|1996-07-22 00:00:00|    3|
    |  10284|1996-08-19 00:00:00|    3|
    |  10267|1996-07-29 00:00:00|    3|
    |  10256|1996-07-15 00:00:00|    3|
    |  10273|1996-08-05 00:00:00|    3|
    +-------+-------------------+-----+
    



```python
p3=p_3.collect()
#p_3.write.save("query3.csv",format="csv")
p3s = pd.DataFrame(p3)
p3s.to_csv("salida3.csv", sep=',',index=False)
```

## Ejercicio 2

#### a. ¿Qué aerolíneas (nombres) llegan al aeropuerto "Honolulu International Airport"?


```python
select distinct airlines.airline from flights join airlines on flights1.airline = airlines.iata_code where flights.destination_airport = "HNL"
```


```python
airlines.createOrReplaceTempView("airlines")
flights.createOrReplaceTempView("flights")
airports.createOrReplaceTempView("airports")
```


```python
p_4 = spark.sql("select distinct airlines.airline from flights join airlines on flights.airline = airlines.iata_code where flights.destination_airport = 'HNL'")
p_4.show()

```

    +--------------------+
    |             airline|
    +--------------------+
    |      Virgin America|
    |United Air Lines ...|
    |     US Airways Inc.|
    |Hawaiian Airlines...|
    |Alaska Airlines Inc.|
    |Delta Air Lines Inc.|
    |American Airlines...|
    +--------------------+
    



```python
p4=p_4.collect()
#p_4.write.save("query4.csv",format="csv")
import pandas as pd
p4s = pd.DataFrame(p4)
p4s.to_csv("salida4.csv", sep=',',index=False)
```

 #### b. ¿En qué horario (hora del día, no importan los minutos) hay salidas del aeropuerto de San Francisco ("SFO") a "Honolulu International Airport"? 


```python
p_5 = spark.sql("select distinct SUBSTRING( scheduled_departure,1,2) as horario from flights where origin_airport == 'SFO' and destination_airport == 'HNL'")
p_5.show()
```

    +-------+
    |horario|
    +-------+
    |     15|
    |     11|
    |     73|
    |     85|
    |     16|
    |     18|
    |     70|
    |     17|
    |     90|
    |     19|
    |     93|
    |     95|
    |     84|
    |     10|
    |     65|
    |     12|
    |     83|
    |     13|
    |     14|
    |     94|
    +-------+
    only showing top 20 rows
    



```python
p5=p_5.collect()
#p_5.write.save("query5.csv",format="csv")
p5s = pd.DataFrame(p5)
p5s.to_csv("salida5.csv", sep=',',index=False)
```

#### c. ¿Qué día de la semana y en qué aerolínea nos conviene viajar a "Honolulu International Airport" para tener el menor retraso posible?


```python
p_6 = spark.sql("select day_of_week, airlines.airline, MIN(departure_delay) AS retraso from flights join airlines on flights.airline = airlines.iata_code where destination_airport == 'HNL' group by day_of_week, airlines.airline order by retraso asc LIMIT 1")
p_6.show()
```

    +-----------+--------------------+-------+
    |day_of_week|             airline|retraso|
    +-----------+--------------------+-------+
    |          5|Hawaiian Airlines...|    -27|
    +-----------+--------------------+-------+
    



```python
p6=p_6.collect()
#p_6.write.save("query6.csv",format="csv")
p6s = pd.DataFrame(p6)
p6s.to_csv("salida6.csv", sep=',',index=False)

```

#### d. ¿Cuál es el aeropuerto con mayor tráfico de entrada?


```python
p_7= spark.sql("select DESTINATION_AIRPORT, airlines.airline, count(*) as trafico from flights join airlines on flights.airline = airlines.iata_code group by DESTINATION_AIRPORT, airlines.airline order by trafico desc limit 1")
p_7.show()
```


```python
p7=p_7.collect()
#p_7.write.save("query7.csv",format="csv")
p7s = pd.DataFrame(p7)
p7s.to_csv("salida7.csv", sep=',',index=False)
```

#### e. ¿Cuál es la aerolínea con mayor retraso de salida por día de la semana?


```python
p_8= spark.sql("select f.day_of_week, f.airline, f.departure_delay from(select day_of_week, max(departure_delay) as retraso from flights group by day_of_week) as x inner join flights as f on f.day_of_week = x.day_of_week and f.departure_delay = x.retraso")
p_8.show()
```


```python
p8=p_8.collect()
#p_8.write.save("query8.csv",format="csv")
p8s = pd.DataFrame(p8)
p8s.to_csv("salida8.csv", sep=',',index=False)
```

#### f. ¿Cuál es la tercer aerolínea con menor retraso de salida los lunes (day of week = 2)? 


```python
p_9=spark.sql("select airlines.airline, min(departure_delay) AS retraso from flights join airlines on flights.airline = airlines.iata_code where day_of_week == 2 group by airlines.airline order by retraso asc limit 2")
p_9.show()
```


```python
p9=p_9.collect()
#p_9.write.save("query9.csv",format="csv")
p9s = pd.DataFrame(p9)
p9s.to_csv("salida9.csv", sep=',',index=False)
```

#### g. ¿Cuál es el aeropuerto origen que llega a la mayor cantidad de aeropuertos destino diferentes?


```python
select origin_airport, count(distinct destination_airport) as total from flights group by origin_airport order by total desc limit 1;
```


```python
p_10 = spark.sql("select origin_airport, count(distinct destination_airport) as total from flights group by origin_airport order by total desc limit 1")
p_10.show()
```

    +--------------+-----+
    |origin_airport|total|
    +--------------+-----+
    |           ATL|  169|
    +--------------+-----+
    



```python
p10=p_10.collect()
#p_9.write.save("query9.csv",format="csv")
p10s = pd.DataFrame(p10)
p10s.to_csv("salida10.csv", sep=',',index=False)
```
