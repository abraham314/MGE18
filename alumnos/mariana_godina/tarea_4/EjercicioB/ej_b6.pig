--ejercicio B6
flights = load 's3://proyectopig/input/flights/flights.csv' using PigStorage(',') as (YEAR:int, MONTH:int, DAY:int, DAY_OF_WEEK:int, AIRLINE:chararray, FLIGHT_NUMBER:chararray, TAIL_NUMBER:chararray, ORIGIN_AIRPORT:chararray, DESTINATION_AIRPORT:chararray, SCHEDULED_DEPARTURE:int, DEPARTURE_TIME:int, DEPARTURE_DELAY:int, TAXI_OUT:int, WHEELS_OFF:int, SCHEDULED_TIME:int, ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, WHEELS_ON:int, TAXI_IN:int, SCHEDULED_ARRIVAL:int, ARRIVAL_TIME:int, ARRIVAL_DELAY:int, DIVERTED:int, CANCELLED:chararray);
airports = load 's3://proyectopig/input/flights/airports.csv' using PigStorage(',') as (IATA_CODE:chararray, AIRPORT:chararray, CITY:chararray, STATE:chararray, COUNTRY:chararray, LATITUDE:float, LONGITUDE:float);
grupo_flights = GROUP flights BY FLIGHT_NUMBER;
unique_dest = FOREACH grupo_flights {unique_destination = DISTINCT flights.DESTINATION_AIRPORT;
GENERATE group as FLIGHT_NUMBER,
COUNT(unique_destination) as sum_unique_destination, unique_destination;};
unique_dest_ord = ORDER unique_dest BY sum_unique_destination DESC;
max_unique_dest = limit unique_dest_ord 1;
store max_unique_dest into 's3://proyectopig/output/ejercicioB/p6' USING PigStorage(',');