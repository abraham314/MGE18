flights = LOAD 's3://aws-alex-03032018-metodos-gran-escala/datos/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD 's3://aws-alex-03032018-metodos-gran-escala/datos/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitud:float, longitude:float);
group_aero_dest = GROUP flights by destination_airport;
order_aero_dest = FOREACH group_aero_dest generate group as destination_airport,COUNT($1) as n;
join_order_aero_dest = JOIN order_aero_dest by destination_airport, airports by iata_code;
honolulu = filter join_order_aero_dest by (airport == 'Honolulu International Airport');
honolulu_final = FOREACH honolulu generate airport,n;
STORE honolulu_final INTO 's3://aws-alex-03032018-metodos-gran-escala/output/pregunta_b1' USING PigStorage(';');
