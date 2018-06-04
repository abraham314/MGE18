/* *EJERCICIO B* */

/* Datos */ 

airlines = load 's3://driflore94maestria/data/airlines.csv'  using PigStorage(',') as (iata_code:chararray,airline:chararray);

airports = load 's3://driflore94maestria/data/airports.csv' using PigStorage(',') as (iata_code:chararray,	airport:chararray,	city:chararray,	state:chararray,	country:chararray,	latitude:float,	longitude:float); 

flights = load 's3://driflore94maestria/data/flights.csv' using PigStorage(',') as (year:int, month:int, day:int,	day_of_week:int,	airline:chararray,	flight_number:int,	tail_number:chararray,	origin_airport:chararray,	destination_airport:chararray,	scheduled_departure:int,	departure_time:int,	departure_delay:int, taxi_out:int,	wheels_off:int,	scheduled_time:int,	elapsed_time:int,	air_time:int,	distance:int,	wheels_on:int,	taxi_in:int,	scheduled_arrival:int, arrival_time:int,	arrival_delay:int,	diverted:int,	cancelled:int,	cancellation_reason:chararray,	air_system_delay:chararray,	security_delay:chararray,	airline_delay:chararray,	late_aircraft_delay:chararray,	weather_delay:chararray);


/* Pregunta 1 */

code_airport = FOREACH airports generate iata_code, airport;
dest = FOREACH flights generate destination_airport;
g_dest = GROUP dest BY destination_airport;
dest_count = FOREACH g_dest GENERATE group as destination_airport, COUNT($1) as n;
count_code_airport = JOIN dest_count BY destination_airport, code_airport by iata_code;
flights_honolulu = FILTER count_code_airport by airport in ('Honolulu International Airport');
result_data = FOREACH flights_honolulu generate $3 as airport, $1 as flights;
store result_data into 's3://driflore94maestria/ej1b' using PigStorage(',', '-schema');


/* Pregunta 2 */

delay = ORDER flights BY arrival_delay DESC;
flights_delay = FOREACH delay GENERATE airline as airline_code, flight_number, arrival_delay;
flights_delay_1 = limit flights_delay 1;
result_data = JOIN flights_delay_1 by airline_code, airlines by iata_code;
store result_data into 's3://driflore94maestria/ej2b' using PigStorage(',', '-schema');

/* Pregunta 3 */

flights_canceled = FOREACH flights GENERATE day_of_week, cancelled;
flights_cfilter = FILTER flights_canceled BY cancelled == 1;
group_cancelled = GROUP flights_cfilter BY day_of_week;
count_day = FOREACH group_cancelled GENERATE group as day_of_week, COUNT($1) as n;
ranked = rank count_day by n DESC;
result_data = limit ranked 1;
store result_data into 's3://driflore94maestria/ej3b' using PigStorage(',', '-schema');

/* Pregunta 4 */
code_airport = FOREACH airports GENERATE iata_code, airport;
cancelled_f = FILTER flights BY cancelled == 1;
origen_cancelled = FOREACH cancelled_f GENERATE origin_airport, cancelled;
group_origen_cancelled = GROUP origen_cancelled BY origin_airport;
sum_cancelled = FOREACH group_origen_cancelled GENERATE group as origin_airport, COUNT($1) as total;
cancelled_17 = FILTER sum_cancelled BY total == 17;
join_cancelled_17 = JOIN cancelled_17 by origin_airport, code_airport by iata_code;
result_data = FOREACH join_cancelled_17 GENERATE airport, total;
store result_data into 's3://driflore94maestria/ej4b' using PigStorage(',', '-schema');

/* Pregunta  5 */
cancelled_f = FILTER flights BY cancelled == 1;
origen_cancelled = FOREACH cancelled_f GENERATE origin_airport, cancelled;
code_airport = FOREACH airports GENERATE iata_code, airport;
group_origen_cancelled = GROUP origen_cancelled BY origin_airport;
sum_cancelled = FOREACH group_origen_cancelled GENERATE group as origin_airport, COUNT($1) as n;
join_more_cancelled = JOIN sum_cancelled by origin_airport, code_airport by iata_code;
ranked_join = RANK join_more_cancelled by n DESC;
limited_rank = LIMIT ranked_join 1;
result_data = FOREACH limited_rank generate airport as airport, n as total_cancellations;
store result_data into 's3://driflore94maestria/ej5b' using PigStorage(',', '-schema');

/* Ejercicio 6 */
destinos = FOREACH flights GENERATE flight_number, destination_airport;
destinos_distintos = DISTINCT destinos;
code_airport = FOREACH airports GENERATE iata_code, airport;
group_destinos = GROUP destinos by flight_number;
unique_airports = FOREACH group_destinos {arpdest = destinos.destination_airport; unique_arpdest = distinct arpdest; GENERATE group as flight_number, COUNT(unique_arpdest) as n;};
rank_destinos = ORDER unique_airports by n DESC;
limit_rank = LIMIT rank_destinos 1;
join_limit_rank = JOIN limit_rank by flight_number, destinos_distintos by flight_number;
join_limit_codeairport = JOIN join_limit_rank by $3, code_airport BY iata_code;
result_data = FOREACH join_limit_codeairport GENERATE $0 as vuelo, $1 as qty, $5 as aeropuerto;  
store result_data into 's3://driflore94maestria/ej6b' using PigStorage(',', '-schema');

