flights = LOAD 's3://aws-alex-03032018-metodos-gran-escala/datos/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD 's3://aws-alex-03032018-metodos-gran-escala/datos/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitud:float, longitude:float);
vuelos_cancelados = filter flights by (cancelled==1);
group_cancel = GROUP vuelos_cancelados by origin_airport;
count_cancel = FOREACH group_cancel generate group as origin_airport, COUNT ($1) as n;
aeropuerto_17 = filter count_cancel by (n == 17);
join_aeropuerto_17 = JOIN aeropuerto_17 by origin_airport, airports by iata_code;
STORE join_aeropuerto_17 INTO 's3://aws-alex-03032018-metodos-gran-escala/output/pregunta_b4' USING PigStorage(';');
