flights = LOAD 's3://granescala-t4/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:int, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);

airports = LOAD 's3://granescala-t4/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitude:float, longitude:float);

vuelos_airp = JOIN flights by origin_airport, airports by iata_code;

v_cancel = FILTER vuelos_airp by cancelled == 1;

cancelados = GROUP v_cancel by airport;

cuenta = FOREACH cancelados GENERATE group as airport, COUNT($1) as n;

peor = FILTER cuenta by n==17;

res = FOREACH peor GENERATE airport, n;

STORE res INTO 's3://granescala-t4/resultadoejercicio4.csv' using PigStorage(',');
