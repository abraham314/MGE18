--ejercicio B3
flights = load 's3://proyectopig/input/flights/flights.csv' using PigStorage(',') as (YEAR:int, MONTH:int, DAY:int, DAY_OF_WEEK:int, AIRLINE:chararray, FLIGHT_NUMBER:chararray, TAIL_NUMBER:chararray, ORIGIN_AIRPOR:chararray, DESTINATION_AIRPORT:chararray, SCHEDULED_DEPARTURE:int, DEPARTURE_TIME:int, DEPARTURE_DELAY:int, TAXI_OUT:int, WHEELS_OFF:int, SCHEDULED_TIME:int, ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, WHEELS_ON:int, TAXI_IN:int, SCHEDULED_ARRIVAL:int, ARRIVAL_TIME:int, ARRIVAL_DELAY:int, DIVERTED:int, CANCELLED:chararray);
cancelados = FILTER flights BY CANCELLED == '1';
group_dia = GROUP cancelados BY DAY_OF_WEEK;
count_flights = FOREACH group_dia GENERATE group as DAY_OF_WEEK, COUNT(cancelados) as n;
ranked = rank count_flights by n DESC;
limited_rank = limit ranked 1;
store limited_rank into 's3://proyectopig/output/ejercicioB/p3' USING PigStorage(',');
