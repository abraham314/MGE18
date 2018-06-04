aerolineas = load 's3://metodosgranescalatarea4/inputs/airlines.csv' using PigStorage(',') as (iata_code:chararray,airline:chararray);

aeropuertos = load 's3://metodosgranescalatarea4/inputs/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitude:float, longitude:float);

vuelos = load 's3://metodosgranescalatarea4/inputs/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:int, departure_time:int, departure_delay:int, taxi_out:int,  wheels_off:int, scheduled_time:int, elapsed_time:int, air_time:int,   distance:int, wheels_on:int, taxi_in:int, scheduled_arrival:int, arrival_time:int, arrival_delay:int, diverted:int, cancelled:int, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);


/* Ejercicio b1 */
aeropuerto_hono = filter aeropuertos by airport in ('Honolulu International Airport');
formato_hono = FOREACH aeropuerto_hono generate iata_code, airport;
vuelos_hono = JOIN flights BY destination_airport, formato_hono BY iata_code;
vuelos_hono_agrupados = GROUP vuelos_hono ALL;
result_ejercicio1 = FOREACH vuelos_hono_agrupados GENERATE COUNT(vuelos_hono);
store result_ejercicio1 into 's3://metodosgranescalatarea4/results_ejercicio_b1/' using PigStorage(',', '-schema');


/* Ejercicio b2*/
retrasos_de_llegada = ORDER flights BY arrival_delay DESC;
retrasos_ordenados = FOREACH retrasos_de_llegada GENERATE airline as airline_code, flight_number, arrival_delay;
retrasos_top1 = limit retrasos_ordenados 1;
result_ejercicio2 = JOIN retrasos_top1 by airline_code,airlines by iata_code;
store result_ejercicio2 into 's3://metodosgranescalatarea4/results_ejercicio_b2/' using PigStorage(',', '-schema');


/*Ejercicio b3*/
cancelados = filter flights by cancelled == 1;
solo_dia = FOREACH cancelados GENERATE day_of_week, cancelled;
agrupado_por_dia = GROUP solo_dia by day_of_week;
cuenta_por_dia = FOREACH agrupado_por_dia GENERATE group as day_of_week, COUNT($1) as n;
result_ejercicio3 = rank cuenta_por_dia by n DESC;
result_ej3 = limit result_ejercicio3 1;
store result_ej3 into 's3://metodosgranescalatarea4/results_ejercicio_b3/' using PigStorage(',', '-schema');

/*Ejercicio b4*/
cancelados_por_aeropuerto = FOREACH cancelados GENERATE origin_airport, cancelled;
grupo_de_cancelados = GROUP cancelados_por_aeropuerto by origin_airport;
suma_de_cancelados = FOREACH grupo_de_cancelados GENERATE group as origin_airport, COUNT($1) as n;
cancelados_de_17 = filter suma_de_cancelados by n == 17;
cancelados_unidos = JOIN cancelados_de_17 by origin_airport, aeropuertos by iata_code;
result_ejercicio4 = FOREACH cancelados_unidos GENERATE iata_code, airport, n;
store result_ejercicio4 into 's3://metodosgranescalatarea4/results_ejercicio_b4/' using PigStorage(',', '-schema');


/*Ejercicio b5*/
cancelados_unidos = JOIN suma_de_cancelados by origin_airport, aeropuertos by iata_code;
cancelados_ordenados = ORDER cancelados_unidos by n DESC;
result_ejercicio5 = limit cancelados_ordenados 1;
store result_ejercicio5 into 's3://metodosgranescalatarea4/results_ejercicio_b5/' using PigStorage(',', '-schema');

/*Ejercicio b6*/
destinos = FOREACH flights GENERATE flight_number, destination_airport;
destinos_unicos = distinct destinos;
informacion_aeropuertos = FOREACH aeropuertos GENERATE iata_code,airport;
vuelos_agrupados = GROUP destinos by flight_number;
aeropuertos_unicos = FOREACH vuelos_agrupados {dst = destinos.destination_airport; unique_airport = distinct dst; GENERATE group as flight_number, COUNT(unique_airport) as n;};
mayores_destinos = ORDER aeropuertos_unicos by n DESC;
mayor_destino = limit mayores_destinos 1;
mayor_destino_iata = JOIN mayor_destino by flight_number,destinos_unicos by flight_number; 
mayor_destino_vuelos = JOIN mayor_destino_iata by $3,aeropuertos by iata_code;
result_ejercicio6 = FOREACH mayor_destino_vuelos GENERATE $0 as flightnumber,$1 as flights_quantity,$3 as iata_code, $5 as airline; 
store result_ejercicio6 into 's3://metodosgranescalatarea4/results_ejercicio_b6/' using PigStorage(',', '-schema');