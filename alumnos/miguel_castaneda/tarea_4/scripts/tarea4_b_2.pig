airlines = LOAD 's3://metodosgranescala/flights/airports.csv' using PigStorage(',') as (iata_code:chararray, airline:chararray);
flights = LOAD 's3://metodosgranescala/flights/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:chararray, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
flights_airlines = JOIN flights BY airline LEFT OUTER,  airlines BY iata_code;
flights_delay = ORDER flights_airlines BY arrival_delay DESC;
respuesta_2 = LIMIT flights_delay 1;
STORE respuesta_2 INTO 's3://metodosgranescala/output/flights/2' USING PigStorage(',');
