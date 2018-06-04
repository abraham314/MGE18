/* Cargamos los datos */


aeropuertos = load 's3://esthefany-dpa/tarea4/input/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitude:float, longitude:float);

vuelos = load 's3://esthefany-dpa/tarea4/input/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:int, departure_time:int, departure_delay:int, taxi_out:int,  wheels_off:int, scheduled_time:int, elapsed_time:int, air_time:int,   distance:int, wheels_on:int, taxi_in:int, scheduled_arrival:int, arrival_time:int, arrival_delay:int, diverted:int, cancelled:int, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);

aerolineas = load 's3://esthefany-dpa/tarea4/input/airlines.csv' using PigStorage(',') as (iata_code:chararray,airline:chararray);


/* Ejercicio b1 */
aeropuerto_honolulu = filter aeropuertos by airport in ('Honolulu International Airport');
formato_honolulu = FOREACH aeropuerto_honolulu generate iata_code, airport;
vuelos_honolulu = JOIN vuelos BY destination_airport, formato_honolulu BY iata_code;
vuelos_honolulu_agrupados = GROUP vuelos_honolulu ALL;
result_ej1 = FOREACH vuelos_honolulu_agrupados GENERATE COUNT(vuelos_honolulu);
store result_ej1 into 's3://esthefany-dpa/tarea4/output/ejercicio_b1' using PigStorage(',', '-schema');


/* Ejercicio b2*/
vuelos_ordenados = ORDER vuelos BY arrival_delay DESC;
max_retraso = LIMIT vuelos_ordenados 1;
result_ej2 = JOIN max_retraso BY airline, aerolineas BY iata_code;
store result_ej2 into 's3://esthefany-dpa/tarea4/output/ejercicio_b2' using PigStorage(',', '-schema');

/*Ejercicio b3*/
cancelados = filter vuelos by cancelled == 1;
cancelados_dia = FOREACH cancelados GENERATE day_of_week, cancelled;
dia_mas = GROUP cancelados_dia by day_of_week;
cuenta_dia = FOREACH dia_mas GENERATE group as day_of_week, COUNT($1) as n;
result_ej3 = rank cuenta_dia by n DESC;
store result_ej3 into 's3://esthefany-dpa/tarea4/output/ejercicio_b3' using PigStorage(',', '-schema');



/*Ejercicio b4*/
cancelados_aeropuerto = FOREACH cancelados GENERATE origin_airport, cancelled;
grupo_cancelados = GROUP cancelados_aeropuerto by origin_airport;
total_cancelados = FOREACH grupo_cancelados GENERATE group as origin_airport, COUNT($1) as n;
cancelados_17 = filter total_cancelados by n == 17;
cancelados_unidos = JOIN cancelados_17 by origin_airport, aeropuertos by iata_code;
result_ej4 = FOREACH cancelados_unidos GENERATE iata_code, airport, n;
store result_ej4 into 's3://esthefany-dpa/tarea4/output/ejercicio_b4' using PigStorage(',', '-schema');


/*Ejercicio b5*/
cancelados_unidos = JOIN total_cancelados by origin_airport, aeropuertos by iata_code;
cancelados_ordenados = ORDER cancelados_unidos by n DESC;
result_ej5 = limit cancelados_ordenados 1;
store result_ej5 into 's3://esthefany-dpa/tarea4/output/ejercicio_b5' using PigStorage(',', '-schema');

/*Ejercicio b6*/

destinos = FOREACH vuelos GENERATE flight_number, destination_airport;
distinct_destinos = distinct destinos;
airports_lista = FOREACH aeropuertos GENERATE iata_code,airport;
flights_grouping = GROUP destinos by flight_number;
airports_unique = FOREACH flights_grouping {dst = destinos.destination_airport; unique_airport = distinct dst; GENERATE group as flight_number, COUNT(unique_airport) as n;};
top_destinos = ORDER airports_unique by n DESC;
top_destiny = limit top_destinos 1;
top_flight_destinies_iata = JOIN top_destiny by flight_number,distinct_destinos by flight_number;
top_flight_destinies_full = JOIN top_flight_destinies_iata by $3,aeropuertos by iata_code;
final = FOREACH top_flight_destinies_full GENERATE $0 as flightnumber,$1 as flights_quantity,$3 as iata_code, $5 as airline;

store final into 's3://esthefany-dpa/tarea4/output/ejercicio_b6' using PigStorage(',', '-schema');