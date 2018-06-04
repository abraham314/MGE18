rmf $OUTPUT

flights = load '$INPUT/flights.csv' using PigStorage(',') as (YEAR:int, MONTH:int, DAY:int, DAY_OF_WEEK:int, AIRLINE:chararray , FLIGHT_NUMBER:int, TAIL_NUMBER:chararray , ORIGIN_AIRPORT:chararray , DESTINATION_AIRPORT:chararray , SCHEDULED_DEPARTURE:chararray , DEPARTURE_TIME:chararray , DEPARTURE_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray , SCHEDULED_TIME:int, ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, WHEELS_ON:chararray , TAXI_IN:int, SCHEDULED_ARRIVAL:chararray , ARRIVAL_TIME:chararray , ARRIVAL_DELAY:int, DIVERTED:int, CANCELLED:int, CANCELLATION_REASON:chararray , AIR_SYSTEM_DELAY:int, SECURITY_DELAY:int, AIRLINE_DELAY:int, LATE_AIRCRAFT_DELAY:int, WEATHER_DELAY:int);

X = FOREACH flights GENERATE FLIGHT_NUMBER, DESTINATION_AIRPORT;

A = distinct X; 
B = group A by FLIGHT_NUMBER;
count_products = FOREACH B GENERATE group, COUNT($1) as n;

Y = filter A by FLIGHT_NUMBER == 202;
store Y into '$OUTPUT';