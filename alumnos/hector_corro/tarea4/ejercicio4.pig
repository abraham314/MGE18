flights = LOAD '$INPUT/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:int, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD '$INPUT/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitude:float, longitude:float);
vuelos_por_airport = JOIN flights by origin_airport, airports by iata_code;
vuelos_cancelados = FILTER vuelos_por_airport by cancelled == 1;
cancelados = GROUP vuelos_cancelados by airport;
cuenta = FOREACH cancelados GENERATE group as airport, COUNT($1) as n;
malos = FILTER cuenta by n==17;
resultado = FOREACH malos GENERATE airport, n;
store resultado into '$OUTPUT/resultado4/' using PigStorage(',');