flights = LOAD 's3://tarea4/bases/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);

airports = LOAD 's3://tarea4/bases/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitud:float, longitude:float);

cancel = filter flights by (cancelled==1);

cancel_aigrp = group cancel by origin_airport; 

canaip = FOREACH cancel_aigrp GENERATE group as origin_airport,COUNT($1) as total; 
can17 = filter canaip by (total==17); 

join_airp = JOIN can17 by origin_airport, airports by iata_code; 

STORE join_airp INTO 's3://tarea4/outputs/ejer_b4' USING PigStorage(',');
