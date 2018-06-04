flights = LOAD 's3://mat34710/tarea4/datos/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:chararray, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD 's3://mat34710/tarea4/datos/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitude:float, longitude:float);
flights_airports = JOIN flights by origin_airport LEFT OUTER, airports by iata_code;
airports_cancelled = FILTER flights_airports by cancelled in ('1');
airports_cancelaciones = GROUP airports_cancelled by airport;
total_cancelaciones = FOREACH airports_cancelaciones GENERATE group as airport, COUNT($1) as cancelaciones;
respuesta = FILTER total_cancelaciones by cancelaciones == 17;
STORE respuesta INTO 's3://mat34710/tarea4/outputs/preguntaB4' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
