flights = LOAD 's3://metodosgranescala/flights/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:chararray, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
cancelados = FILTER flights BY cancelled == '1';
cancelados_dow = GROUP cancelados by day_of_week;
totales_dow = FOREACH cancelados_dow GENERATE group as day_of_week, COUNT($1) as total;
cancelados_o = ORDER totales_dow by total DESC;
respuesta_3 = LIMIT cancelados_o 1;
STORE respuesta_3 INTO 's3://metodosgranescala/output/flights/3' USING PigStorage(',');
