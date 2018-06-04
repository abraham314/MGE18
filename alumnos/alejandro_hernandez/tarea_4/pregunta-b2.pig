flights = LOAD 's3://aws-alex-03032018-metodos-gran-escala/datos/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airlines = LOAD 's3://aws-alex-03032018-metodos-gran-escala/datos/airlines.csv' using PigStorage(',') as (iata_code:chararray, airline:chararray);
join_flights_airlines = JOIN flights by airline, airlines by iata_code;
rank_retraso = ORDER join_flights_airlines by departure_delay DESC;
max_retraso = limit rank_retraso 1;
retraso = FOREACH max_retraso GENERATE flight_number,airlines::airline,departure_delay;
STORE retraso INTO 's3://aws-alex-03032018-metodos-gran-escala/output/pregunta_b2' USING PigStorage(';');
