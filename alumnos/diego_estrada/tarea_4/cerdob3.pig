flights = LOAD 's3://mat34710/tarea4/datos/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:chararray, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:chararray, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
flights_cancelados = FILTER flights by cancelled in ('1');
cancelados_dayofweek = GROUP flights_cancelados by day_of_week;
totales_dayofweek = FOREACH cancelados_dayofweek GENERATE group as day_of_week, COUNT($1) as total_cancelaciones;
cancellations_order = ORDER totales_dayofweek by total_cancelaciones DESC;
cancellations_limit = limit cancellations_order 1;
STORE cancellations_limit INTO 's3://mat34710/tarea4/outputs/preguntaB3' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
