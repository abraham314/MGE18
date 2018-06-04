--borrar la salida
rmf $OUTPUT

vuelos = load '$INPUT/flights' using PigStorage(',') 
as (YEAR:chararray, MONTH:chararray, DAY:chararray, DAY_OF_WEEK:chararray,AIRLINE:chararray,
    FLIGHT_NUMBER:chararray,TAIL_NUMBER:chararray,ORIGIN_AIRPORT:chararray,DESTINATION_AIRPORT:chararray,
    SCHEDULED_DEPARTURE:int, DEPARTURE_TIME:int, DEPARTURE_DELAY:int,TAXI_OUT: int,WHEELS_OFF:chararray,SCHEDULED_TIME:chararray,
    ELAPSED_TIME:chararray,AIR_TIME:int, DISTANCE: int, WHEELS_ON:chararray, TAXI_IN:int, SCHEDULED_ARRIVAL:chararray, 
    ARRIVAL_TIME:chararray,ARRIVAL_DELAY:int,DIVERTED:chararray,CANCELLED:int,CANCELLATION_REASON:chararray,AIR_SYSTEM_DELAY:chararray,
    SECURITY_DELAY:chararray,AIRLINE_DELAY:chararray,LATE_AIRCRAFT_DELAY:chararray,WEATHER_DELAY:chararray);

filt_hono = FILTER vuelos by DESTINATION_AIRPORT=='HNL';

group_hono = GROUP filt_hono ALL;

count_hono =FOREACH group_hono GENERATE COUNT(filt_hono);

store count_hono into '$OUTPUT' using PigStorage(',', '-schema');