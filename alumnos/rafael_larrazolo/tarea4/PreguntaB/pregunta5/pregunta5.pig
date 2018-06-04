-- pregunta5.pig
flights = LOAD 's3://larrazolo/data/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD 's3://larrazolo/data/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitud:float, longitude:float);
flights_sum = FOREACH flights generate origin_airport, cancelled;
airports_sum = FOREACH airports generate iata_code, airport;
cancelled = FILTER flights_sum by cancelled == 1;
group_origin = group cancelled by origin_airport;
count_cancelled = FOREACH group_origin GENERATE group as origin_airport, COUNT($1) as total_cancel;
join_airports = join count_cancelled by origin_airport, airports_sum by iata_code;
ranked = rank join_airports by total_cancel DESC;
limited = limit ranked 1;
salida = FOREACH limited generate airport as airport, iata_code as code, total_cancel as cancelations;
STORE salida INTO 's3://larrazolo/output/preg5' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

