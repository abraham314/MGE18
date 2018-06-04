flights = LOAD 's3://granescala-t4/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:int, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);

cancelados = FILTER flights by cancelled == 1;

cancelado_diario = GROUP cancelados by day_of_week;

cuenta = FOREACH cancelado_diario GENERATE group as day_of_week, COUNT($1) as n;

top_cancel = rank cuenta by n DESC;

lim_cancel = limit top_cancel 1;

STORE lim_cancel INTO 's3://granescala-t4/resultadoejercicio3.csv' using PigStorage(',');
