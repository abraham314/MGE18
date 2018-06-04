flights = load 's3://audiracmichelle/flights/flights.csv' using PigStorage(',') as (year:int, month:int, day:int, day_of_week:int, airline:chararray, flight_number:int, tail_number:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:int, departure_time:int, departure_delay:int, taxi_out:int, wheels_off:int, scheduled_time:int, elapsed_time:int, air_time:int, distance:int, wheels_on:int, taxi_in:int, scheduled_arrival:int, arrival_time:int, arrival_delay:int, diverted:int, cancelled:int, cancellation_reason:chararray, air_system_delay:chararray,	security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);

group_flights = group flights by origin_airport;

cancellations = foreach group_flights generate $0, SUM(flights.cancelled) as sum_cancelled;
cancellations = filter cancellations by sum_cancelled == 17;

store cancellations into 's3://audiracmichelle/b4/output/' using PigStorage(',');