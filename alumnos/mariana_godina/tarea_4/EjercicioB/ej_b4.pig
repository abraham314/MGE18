--ejercicio B4
flights = load 's3://proyectopig/input/flights/flights.csv' using PigStorage(',') as (YEAR:int, MONTH:int, DAY:int, DAY_OF_WEEK:int, AIRLINE:chararray, FLIGHT_NUMBER:chararray, TAIL_NUMBER:chararray, ORIGIN_AIRPORT:chararray, DESTINATION_AIRPORT:chararray, SCHEDULED_DEPARTURE:int, DEPARTURE_TIME:int, DEPARTURE_DELAY:int, TAXI_OUT:int, WHEELS_OFF:int, SCHEDULED_TIME:int, ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, WHEELS_ON:int, TAXI_IN:int, SCHEDULED_ARRIVAL:int, ARRIVAL_TIME:int, ARRIVAL_DELAY:int, DIVERTED:int, CANCELLED:chararray);
airports = load 's3://proyectopig/input/flights/airports.csv' using PigStorage(',') as (IATA_CODE:chararray, AIRPORT:chararray, CITY:chararray, STATE:chararray, COUNTRY:chararray, LATITUDE:float, LONGITUDE:float);
cancelados = FILTER flights BY CANCELLED == '1';
group_aero_or = GROUP cancelados BY ORIGIN_AIRPORT;
count_flights = FOREACH group_aero_or GENERATE group as ORIGIN_AIRPORT, COUNT(cancelados) as n;
aero_can = FILTER count_flights BY n == 17;
salida = join airports by IATA_CODE, aero_can by ORIGIN_AIRPORT;
store salida into 's3://proyectopig/output/ejercicioB/p4' USING PigStorage(',');
