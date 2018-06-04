flights = LOAD 's3://granescala-t4/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:int, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);

airports = LOAD 's3://granescala-t4/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitude:float, longitude:float);

vuelos_aer = JOIN flights BY destination_airport, airports by iata_code;

agrvuelos = GROUP vuelos_aer by flight_number;

uniq = FOREACH agrvuelos { destino = FOREACH vuelos_aer GENERATE destination_airport; destino_unico = DISTINCT destino; GENERATE flatten(group) as flight_number, COUNT(destino_unico)as n;};

dest = ORDER uniq BY n DESC;

sort_standing = LIMIT dest 1;

other = DISTINCT flights;

vuelosfin = JOIN sort_standing by flight_number, other by flight_number;

res = JOIN vuelosfin by destination_airport LEFT OUTER, airports by iata_code;

STORE res INTO 's3://granescala-t4/resultadoejercicio6.csv' using PigStorage(',');