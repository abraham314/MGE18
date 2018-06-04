-- pregunta3.pig
flights = LOAD 's3://larrazolo/data/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
flights_sum = FOREACH flights generate day_of_week, cancelled;
cancelled = FILTER flights_sum by cancelled == 1;
group_day = group cancelled by day_of_week;
count_cancelled = FOREACH group_day GENERATE group as day_of_week, COUNT($1) as total_cancel;
ranked = rank count_cancelled by total_cancel DESC;
limited = limit ranked 1;
salida = FOREACH limited generate day_of_week, total_cancel as cancelations_on_dayweek;
STORE salida INTO 's3://larrazolo/output/preg3' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

