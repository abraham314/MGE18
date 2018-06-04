flights = LOAD 's3://mat34710/tarea4/datos/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline_code:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:chararray, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airlines = LOAD 's3://mat34710/tarea4/datos/airlines.csv' using PigStorage(',') as (iata_code:chararray, airline:chararray);
flights_airlines = JOIN flights by airline_code left outer, airlines by iata_code;
arrival_delays = ORDER flights_airlines by arrival_delay DESC;
worst_delay = limit arrival_delays 1;
only_interest = FOREACH worst_delay GENERATE iata_code, airline, flight_number, arrival_delay;
STORE only_interest INTO 's3://mat34710/tarea4/outputs/preguntaB2' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
