/******************  Ejercicio B   ************************/
/******************  Cargamos datasets de s3 **************/
airlines = load 's3://al102964-bucket1/tarea4/ejercicio_b/airlines.csv'  using PigStorage(',') as (iata_code:chararray,airline:chararray);

airports = load 's3://al102964-bucket1/tarea4/ejercicio_b/airports.csv' using PigStorage(',') as (iata_code:chararray,	airport:chararray,	city:chararray,	state:chararray,	country:chararray,	latitude:float,	longitude:float); 

flights = load 's3://al102964-bucket1/tarea4/ejercicio_b/flights.csv' using PigStorage(',') as (year:int, month:int, day:int,	day_of_week:int,	airline:chararray,	flight_number:int,	tail_number:chararray,	origin_airport:chararray,	destination_airport:chararray,	scheduled_departure:int,	departure_time:int,	departure_delay:int, taxi_out:int,	wheels_off:int,	scheduled_time:int,	elapsed_time:int,	air_time:int,	distance:int,	wheels_on:int,	taxi_in:int,	scheduled_arrival:int, arrival_time:int,	arrival_delay:int,	diverted:int,	cancelled:int,	cancellation_reason:chararray,	air_system_delay:chararray,	security_delay:chararray,	airline_delay:chararray,	late_aircraft_delay:chararray,	weather_delay:chararray);


/* Ejercicio 1 */
/*¿Cuantos vuelos existen en el dataset cuyo aeropuerto destino sea el "Honolulu International Airport"? */
honolulu = filter airports by airport in ('Honolulu International Airport');
honolulu_format = FOREACH honolulu generate iata_code, airport;
honolulu_flights = JOIN flights BY destination_airport, honolulu_format BY iata_code;
honolulu_flights_group = GROUP honolulu_flights ALL;
hlf_count = FOREACH honolulu_flights_group GENERATE COUNT(honolulu_flights);
store hlf_count into 's3://al102964-bucket1/tarea4/ejercicio_b/output_ej1/' using PigStorage(',', '-schema');


/* Ejercicio 2*/
/*¿Cuál es el vuelo con más retraso? ¿De qué aerolínea es?*/
flights_arrival_delay = ORDER flights BY arrival_delay DESC;
flights_summary = FOREACH flights_arrival_delay GENERATE airline as airline_code, flight_number, arrival_delay;
flights_summary_top = limit flights_summary 10;
flights_summary_complete = JOIN flights_summary_top by airline_code,airlines by iata_code;
store flights_summary_complete into 's3://al102964-bucket1/tarea4/ejercicio_b/output_ej2/' using PigStorage(',', '-schema');


/*Ejercicio 3*/
/*¿Qué día es en el que más vuelos cancelados hay?*/
cancelados = filter flights by cancelled == 1;
cancelados_reduced = FOREACH cancelados GENERATE day_of_week, cancelled;
dia_mas_group = GROUP cancelados_reduced by day_of_week;
dia_count = FOREACH dia_mas_group GENERATE group as day_of_week, COUNT($1) as n;
ranked = rank dia_count by n DESC;
store ranked into 's3://al102964-bucket1/tarea4/ejercicio_b/output_ej3/' using PigStorage(',', '-schema');


/*Ejercicio 4*/
/*¿Cuáles son los aeropuertos orígen con 17 cancelaciones?*/
cancelados_airports_reduced = FOREACH cancelados GENERATE origin_airport, cancelled;
cancelados_airports_group = GROUP cancelados_airports_reduced by origin_airport;
cancelados_airports_sum = FOREACH cancelados_airports_group GENERATE group as origin_airport, COUNT($1) as n;
cancelled_17 = filter cancelados_airports_sum by n == 17;
cancelled_17_join = JOIN cancelled_17 by origin_airport, airports by iata_code;
cancelled_17_out = FOREACH cancelled_17_join GENERATE iata_code, airport, n;
store cancelled_17_out into 's3://al102964-bucket1/tarea4/ejercicio_b/output_ej4/' using PigStorage(',', '-schema');


/*Ejercicio 5*/
/*¿Cuál es el aeropuerto origen con más vuelos cancelados?*/
cancelled_airports_join = JOIN cancelados_airports_sum by origin_airport, airports by iata_code;
rank_ap_mas_cancelados = ORDER cancelled_airports_join by n DESC;
rank_ap_mas_cancelados_top = limit rank_ap_mas_cancelados 10;
store rank_ap_mas_cancelados_top into 's3://al102964-bucket1/tarea4/ejercicio_b/output_ej5/' using PigStorage(',', '-schema');

/*Ejercicio 6*/
/*¿Cuál es el vuelo (flight number) con mayor diversidad de aeropuertos */
flights_destinations = FOREACH flights GENERATE flight_number, destination_airport;
distinct_flights_destinations = distinct flights_destinations;
airports_data = FOREACH airports GENERATE iata_code,airport;
flights_grouping = GROUP flights_destinations by flight_number;
airports_unique = FOREACH flights_grouping {dst = flights_destinations.destination_airport; unique_airport = distinct dst; GENERATE group as flight_number, COUNT(unique_airport) as n;};
top_destinies = ORDER airports_unique by n DESC;
top_destiny = limit top_destinies 1;
top_flight_destinies_iata = JOIN top_destiny by flight_number,distinct_flights_destinations by flight_number; 
top_flight_destinies_full = JOIN top_flight_destinies_iata by $3,airports by iata_code;
final = FOREACH top_flight_destinies_full GENERATE $0 as flightnumber,$1 as flights_quantity,$3 as iata_code, $5 as airline; 
store final into 's3://al102964-bucket1/tarea4/ejercicio_b/output_ej6/' using PigStorage(',', '-schema');
