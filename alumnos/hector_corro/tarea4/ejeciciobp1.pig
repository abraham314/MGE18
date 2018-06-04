airports = LOAD '$INPUT/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitude:float, longitude:float);
flights = LOAD '$INPUT/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:chararray, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
vuelos_por_aeropuerto = JOIN flights BY destination_airport , airports BY iata_code;
honolulu = FILTER vuelos_por_aeropuerto BY airport == 'Honolulu International Airport';
agrupa_honolulu = GROUP honolulu by airport;
honolulu_ordenado = FOREACH agrupa_honolulu GENERATE group as airport, COUNT($1) as n;
STORE honolulu_ordenado INTO '$OUTPUT/respuesta1/' USING PigStorage(',');
