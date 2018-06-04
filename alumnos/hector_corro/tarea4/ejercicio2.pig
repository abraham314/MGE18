flights = LOAD '$INPUT/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time:chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:chararray, elapsed_time:int, air_time:int, distance:int, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:chararray, cancelled:chararray, cancellation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airlines =LOAD '$INPUT/airlines.csv' using PigStorage(',') as (iata_code:chararray,airline_name:chararray);
vuelo_aerolinea = JOIN flights by airline, airlines by iata_code;
aerolinea_ordenada = ORDER vuelo_aerolinea BY arrival_delay DESC;
demora = LIMIT aerolinea_ordenada 1;
resultado = FOREACH demora GENERATE flight_number, airline_name, iata_code;
store resultado into '$OUTPUT/respuesta2/'  using PigStorage(',');
