flights = LOAD 's3://tarea4/bases/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD 's3://tarea4/bases/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitud:float, longitude:float);

join_airports = JOIN flights by destination_airport, airports by iata_code;
hn = filter join_airports by (airport=='Honolulu International Airport');
hn_g = GROUP hn by airport;
hn_count= FOREACH hn_g generate group as airport,COUNT($1) as total;

STORE hn_count INTO 's3://tarea4/outputs/ejer_b1' USING PigStorage(',');


