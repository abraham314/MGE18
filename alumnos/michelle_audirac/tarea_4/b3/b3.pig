flights = load 's3://audiracmichelle/flights/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:int, departure_time:int, departure_delay:int, taxi_out:int, wheels_off:int, scheduled_time:int, elapsed_time:int, air_time:int, distance:int, wheels_on:int, taxi_in:int, scheduled_arrival:int, arrival_time:int, arrival_delay:int, diverted:int, cancelled:int, cancellation_reason:chararray, air_system_delay:chararray,	security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);

group_flights = group flights by (year, month, day);

cancellations = foreach group_flights generate group.year, group.month, group.day, SUM(flights.cancelled) as sum_cancelled;

cancellations = order cancellations by sum_cancelled DESC;
max = limit cancellations 5;

store max into 's3://audiracmichelle/b3/output/' using PigStorage(',');