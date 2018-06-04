-- pregunta6.pig

flights = LOAD 's3://larrazolo/data/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD 's3://larrazolo/data/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitud:float, longitude:float);
flights_sum = FOREACH flights generate flight_number, destination_airport;
codes = FOREACH airports generate iata_code, airport;
group_flight = group flights_sum by flight_number;
uniq_airport = FOREACH group_flight {
               destiny = flights_sum.destination_airport;
               uniq_air = distinct destiny;
               generate group as flight_number, COUNT(uniq_air) as n;
};
ranked = rank uniq_airport by n DESC;
limited = limit ranked 1;
distinct_flights_dest = distinct flights_sum;
join_flights = join limited by flight_number, distinct_flights_dest by flight_number; 
join_names = join join_flights by destination_airport LEFT OUTER, codes by iata_code;
salida = FOREACH join_names generate $1 as flight_number, $2 as number_of_distinct_destinations, $4 as iata_code, $6 as destination_airport;
STORE salida INTO 's3://larrazolo/output/preg6' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

