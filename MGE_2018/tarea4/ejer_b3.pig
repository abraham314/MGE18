flights = LOAD 's3://tarea4/bases/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);

cancel = filter flights by (cancelled==1);
cancel_grp = group cancel by day_of_week;
canday = FOREACH cancel_grp GENERATE group as day_of_week,COUNT($1) as total;
ranking =  order canday by total DESC;
top1 = limit ranking 1;
selected = FOREACH top1 GENERATE day_of_week,total;

STORE selected INTO 's3://tarea4/outputs/ejer_b3' USING PigStorage(',');

