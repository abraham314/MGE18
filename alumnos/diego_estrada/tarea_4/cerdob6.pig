flights = LOAD 's3://mat34710/tarea4/datos/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:int, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD 's3://mat34710/tarea4/datos/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitude:float, longitude:float);
flights_airports = JOIN flights by destination_airport LEFT OUTER, airports by iata_code;
group_flights = GROUP flights_airports by flight_number;
list_nested_each = FOREACH group_flights
				{
					list_inner_each = FOREACH flights_airports GENERATE destination_airport;
					list_inner_dist = DISTINCT list_inner_each;
					GENERATE flatten(group) as flight_number, COUNT(list_inner_dist) as uniq_destination_airport;
				};
list_nested_ordered = ORDER list_nested_each by uniq_destination_airport DESC;
winner = limit list_nested_ordered 1;
flights_fields = FOREACH flights generate flight_number, destination_airport;
airports_fields = FOREACH airports generate iata_code, airport;
destinos_unicos = distinct flights_fields;
winner_with_flights = join winner by flight_number, destinos_unicos by flight_number;
result = join winner_with_flights by destination_airport LEFT OUTER, airports_fields by iata_code;
STORE result INTO 's3://mat34710/tarea4/outputs/preguntaB6' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
