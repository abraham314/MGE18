-- pregunta1.pig
flights = LOAD 's3://larrazolo/data/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD 's3://larrazolo/data/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitud:float, longitude:float);
destination = FOREACH flights generate destination_airport;
airport_code = FOREACH airports generate iata_code, airport;
group_destination = group destination by destination_airport;
count_destination = FOREACH group_destination GENERATE group as destination_airport, COUNT($1) as n;
join_airport = JOIN count_destination by destination_airport, airport_code by iata_code;
honolulu = FILTER join_airport by airport in ('Honolulu International Airport');
salida = FOREACH honolulu generate airport as airport, n as flights;
STORE salida INTO 's3://larrazolo/output/preg1' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

