flights = load 's3://audiracmichelle/flights/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:int, departure_time:int, departure_delay:int, taxi_out:int, wheels_off:int, scheduled_time:int, elapsed_time:int, air_time:int, distance:int, wheels_on:int, taxi_in:int, scheduled_arrival:int, arrival_time:int, arrival_delay:int, diverted:int, cancelled:int, cancellation_reason:chararray, air_system_delay:chararray,	security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);

flights = foreach flights generate origin_airport;
HNL = filter flights by origin_airport in ('HNL');

group_HNL = group HNL by $0;
n_HNL = FOREACH group_HNL GENERATE COUNT($1);

store n_HNL into 's3://audiracmichelle/b1/output/' using PigStorage(',');