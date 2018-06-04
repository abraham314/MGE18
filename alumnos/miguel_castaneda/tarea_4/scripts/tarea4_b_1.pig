airports = LOAD 's3://metodosgranescala/flights/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitude:float, longitude:float);
flights = LOAD 's3://metodosgranescala/flights/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:chararray, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
flights_airports = JOIN flights BY destination_airport , airports BY iata_code;
fligths_honolulu = FILTER flights_airports BY airport == 'Honolulu International Airport';
fligths_honolulu_g = GROUP fligths_honolulu by airport;
fligths_honolulu_c = FOREACH fligths_honolulu_g GENERATE group as airport, COUNT($1) as n;
STORE fligths_honolulu_c INTO 's3://metodosgranescala/output/flights/1' USING PigStorage(',');
