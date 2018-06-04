flights = LOAD '$INPUT/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:int, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD '$INPUT/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitude:float, longitude:float);

vuelos_aeropuerto = JOIN flights BY destination_airport, airports by iata_code;
agrupa_vuelos = GROUP vuelos_aeropuerto by flight_number;
unicos = FOREACH agrupa_vuelos { destino = FOREACH vuelos_aeropuerto GENERATE destination_airport; destino_unico = DISTINCT destino; GENERATE flatten(group) as flight_number, COUNT(destino_unico)as n;};
ordena_destino = ORDER unicos BY n DESC;
ordena_standing = LIMIT ordena_destino 1;
otros_vuelos = DISTINCT flights;
vuelos_varios = JOIN ordena_standing by flight_number, otros_vuelos by flight_number;
resultado = JOIN vuelos_varios by destination_airport LEFT OUTER, airports by iata_code;
store resultado into '$OUTPUT/resultado6/' using PigStorage(',');
