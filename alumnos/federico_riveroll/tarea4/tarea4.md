
## Federico Riveroll 
# Tarea 4

### Ejercicio 1

Para éste ejercicio se creó un cluster (EMR) en AWS y vía la página se habilitó SSH, 

<img width="50%" src="img/vista_sitio"/>



Y luego vía la consola, con la llave asignada se subieron las tablas necesarias (products.csv y order_details y el archivo <b>pig (uno.pig)</b>) con el comando SCP (por ejemplo):

<code>scp -i clustergranescala.pem /home/federico/Documents/MaestriaCDat2/GRAN_ESCALA_M/tarea4/products.csv hadoop@ec2-34-229-183-158.compute-1.amazonaws.com:~/archivos
</code>

Por tanto, en el directorio de la máquina virtual va a haber una carpeta "archivos" con las tres cosas:

<img width="50%" src="img/subir_archivos"/>

Después, lo que hacemos es conectarnos a la máquina y vemos que ahí están los archivos:

<img width="50%" src="img/conectar_y_ver"/>

Se ejecuta el archivo uno.pig con comando pig:
<img width="50%" src="img/ejecucion_pig"/>
Resultado en particion unica:
<img width="50%" src="img/resultado"/>
Archivo <b>uno.pig</b>:

<code>
products = load 'archivos/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);
order_details = load 'archivos/order_details.csv' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);
group_orders = group order_details by productid;
count_products = FOREACH group_orders GENERATE group as productid, COUNT($1) as n;
ranked = rank count_products by n DESC;
limited_rank = limit ranked 10;
join_order_products = JOIN limited_rank by productid, products by productid;
res = FOREACH join_order_products generate rank_count_products,productname,n;
res_ord = ORDER res by $0;
result = limit res_ord 1;
STORE result INTO 'archivos/pregunta_a' USING PigStorage(';');

</code>

### Ejercicio 2
Se subieron los archivos exactamente igual que los mencionados anteriormente.<br>
<img width="50%" src="img/sube_archivos_vuelos"/>
1) ¿Cuántos vuelos existen en el dataset cuyo aeropuerto destino sea el "Honolulu International Airport"?


Archivo <b>dos_1.pig</b>
<code>
airports = load 'archivos/airports.csv' using PigStorage(',') as (IATA_CODE:chararray, AIRPORT:chararray);
flights = load 'archivos/flights.csv' using PigStorage(',') as (DESTINATION_AIRPORT:chararray);
join_airports_flights = JOIN flights by DESTINATION_AIRPORT, airports by IATA_CODE;
result_0 = FILTER join_airports_flights BY DESTINATION_AIRPORT == 'Honolulu International Airport';
result_1 = GROUP result_0 by AIRPORT;
result_2 = FOREACH result_1 GENERATE group as AIRPORT,COUNT(result_0) as cnt;
result = FOREACH result_2 GENERATE AIRPORT, cnt;
STORE result INTO 'archivos/pregunta_b_1' USING PigStorage(';');
</code>

Se encontraron 43,157 registros
<code>
Honolulu International Airport,43157
</code>

2) ¿Cuál es el vuelo con más retraso? ¿De qué aerolínea es?

Archivo <b>dos_2.pig</b>
<code>
flights = load 'archivos/flights.csv' using PigStorage(',') as (AIRLINE_CODE:chararray, ARRIVAL_DELAY:int);
airlines = load 'archivos/airlines.csv' using PigStorage(',') as (IATA_CODE:chararray, AIRLINE:chararray);
flights_airlines = JOIN flights by AIRLINE_CODE left outer, airlines by IATA_CODE;
delays = ORDER flights_airlines by ARRIVAL_DELAY DESC;
delay_2 = limit delays 1;
only_interest = FOREACH delay_2 GENERATE IATA_CODE, AIRLINE, ARRIVAL_DELAY;
STORE result INTO 'archivos/pregunta_b_2' USING PigStorage(';');</code>

El vuelo se atrasó 1971 unidades de tiempo
<code>
AA,American Airlines Inc.,1971
</code>

3) ¿Qué día es en el que más vuelos cancelados hay?

Archivo <b>dos_3.pig</b>
<code>
flights = load 'archivos/flights.csv' using PigStorage(',') as (AIRLINE_CODE:chararray, ARRIVAL_DELAY:int, 
                                                                CANCELLED:chararray, DAY_OF_WEEK:chararray);
cancelados = FILTER flights by CANCELLED in ('1');
cancelados_dayofweek = GROUP cancelados by DAY_OF_WEEK;
todos_dayofweek = FOREACH cancelados_dayofweek GENERATE group as DAY_OF_WEEK, COUNT($1) as totales;
cancellations_order = ORDER todos_dayofweek by totales DESC;
result = limit cancellations_order 1;
STORE result INTO 'archivos/pregunta_b_3' USING PigStorage(';');
</code>

El día fue 1 (no sé si es Lunes o Domingo) con 21,073 cancelaciones
<code>
1,21073
</code>

4) ¿Cuáles son los aeropuertos orígen con 17 cancelaciones?

Archivo <b>dos_4.pig</b>
<code>
flights = load 'archivos/flights.csv' using PigStorage(',') as (AIRLINE_CODE:chararray, ARRIVAL_DELAY:int, 
                                                                CANCELLED:chararray, DAY_OF_WEEK:chararray,
                                                                ORIGIN_AIRPORT:chararray);
airports = load 'archivos/airports.csv' using PigStorage(',') as (IATA_CODE:chararray, AIRPORT:chararray);

flights_airports = JOIN flights by ORIGIN_AIRPORT LEFT OUTER, airports by IATA_CODE;
cancelados_1 = FILTER flights_airports by CANCELLED in ('1');
cancelados_2 = GROUP cancelados_1 by AIRPORT;
cancelados_3 = FOREACH cancelados_2 GENERATE group as AIRPORT, COUNT($1) as cancelaciones;
result = FILTER cancelados_3 by cancelaciones == 17;
STORE result INTO 'archivos/pregunta_b_4' USING PigStorage(';');
</code>

Aeropuertos orígen con 17 cancelaciones: Dickinson Theodore Roosvelt, Delta Country y Dothal.
<code>
Dickinson Theodore Roosevelt Regional Airport,17
Delta County Airport,17
Dothan Regional Airport,17
</code>

5) ¿Cuál es el aeropuerto origen con más vuelos cancelados?

Archivo <b>dos_5.pig</b>
<code>
flights = load 'archivos/flights.csv' using PigStorage(',') as (AIRLINE_CODE:chararray, ARRIVAL_DELAY:int, 
                                                                CANCELLED:chararray, DAY_OF_WEEK:chararray,
                                                                ORIGIN_AIRPORT:chararray);
airports = load 'archivos/airports.csv' using PigStorage(',') as (IATA_CODE:chararray, AIRPORT:chararray);

flights_airports = JOIN flights by ORIGIN_AIRPORT LEFT OUTER, airports by IATA_CODE;
cancelados_1 = FILTER flights_airports by CANCELLED in ('1');
cancelados_2 = GROUP cancelados_1 by AIRPORT;
cancelados_3 = FOREACH cancelados_2 GENERATE group as AIRPORT, COUNT($1) as cancelaciones;
result_0 = ORDER cancelados_3 by cancelaciones DESC;
result = limit result_0 1;
STORE result INTO 'archivos/pregunta_b_5' USING PigStorage(';');
</code>

El aeropuesrto orígen con más cancelados fué Chicago O'Hare con 8,548 cancelaciones.
<code>
Chicago O'Hare International Airport,8548
</code>

6) ¿Cuál es el vuelo (flight number) con mayor diversidad de aeropuertos destino, cuáles son estos destinos? (ocupar bag te ayudará en esta pregunta)

Archivo <b>dos_6.pig</b>
<code>
flights = load 'archivos/flights.csv' using PigStorage(',') as (AIRLINE_CODE:chararray, ARRIVAL_DELAY:int, 
                                                                CANCELLED:chararray, DAY_OF_WEEK:chararray,
                                                                ORIGIN_AIRPORT:chararray, 
                                                                DESTINATION_AIRPORT:chararray
                                                                FLIGHT_NUMBER:chararray);
airports = load 'archivos/airports.csv' using PigStorage(',') as (IATA_CODE:chararray, AIRPORT:chararray);

flights_airports = JOIN flights by DESTINATION_AIRPORT LEFT OUTER, airports by IATA_CODE;
group_flights = GROUP flights_airports by FLIGHT_NUMBER;
list_nested_each = FOREACH group_flights { list_inner_each = FOREACH flights_airports GENERATE destination_airport; list_inner_dist = DISTINCT list_inner_each; GENERATE flatten(group) as flight_number, COUNT(list_inner_dist) as uniq_destination_airport; };


</code>

No logré que jalara el código en ésta.
