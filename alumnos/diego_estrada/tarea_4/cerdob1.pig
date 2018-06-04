flights = LOAD 's3://mat34710/tarea4/datos/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:chararray, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD 's3://mat34710/tarea4/datos/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitude:float, longitude:float);
flights_airports = JOIN flights by destination_airport LEFT OUTER, airports by iata_code;
flights_airports_filtrado = FILTER flights_airports by airport in ('Honolulu International Airport');
grouped_honolulu = GROUP flights_airports_filtrado by airport;
counted_honolulu = FOREACH grouped_honolulu GENERATE group as airport,COUNT(flights_airports_filtrado) as cnt;
result = FOREACH counted_honolulu GENERATE airport, cnt;
STORE result INTO 's3://mat34710/tarea4/outputs/preguntaB1' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
