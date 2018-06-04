--pregunta2
flights = LOAD 's3://larrazolo/data/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:chararray, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airlines = LOAD 's3://larrazolo/data/airlines.csv' using PigStorage(',') as (iata_code:chararray, airline:chararray);
delays_sum = FOREACH flights GENERATE airline as airline_code, flight_number, arrival_delay;
ranked = rank delays_sum by arrival_delay DESC;
limited = limit ranked 1;
join_airline = JOIN limited by airline_code, airlines by iata_code;
salida = FOREACH join_airline generate flight_number as flight, iata_code as code, airline as airline,  arrival_delay as arrival_delay;
STORE salida INTO 's3://larrazolo/output/preg2' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
