
vuelos = load '$INPUT/flights' using PigStorage(',') 
as (YEAR:chararray, MONTH:chararray, DAY:chararray, DAY_OF_WEEK:chararray,AIRLINE:chararray,
    FLIGHT_NUMBER:chararray,TAIL_NUMBER:chararray,ORIGIN_AIRPORT:chararray,DESTINATION_AIRPORT:chararray,
    SCHEDULED_DEPARTURE:int, DEPARTURE_TIME:int, DEPARTURE_DELAY:int,TAXI_OUT: int,WHEELS_OFF:chararray,SCHEDULED_TIME:chararray,
    ELAPSED_TIME:chararray,AIR_TIME:int, DISTANCE: int, WHEELS_ON:chararray, TAXI_IN:int, SCHEDULED_ARRIVAL:chararray, 
    ARRIVAL_TIME:chararray,ARRIVAL_DELAY:int,DIVERTED:chararray,CANCELLED:int,CANCELLATION_REASON:chararray,AIR_SYSTEM_DELAY:chararray,
    SECURITY_DELAY:chararray,AIRLINE_DELAY:chararray,LATE_AIRCRAFT_DELAY:chararray,WEATHER_DELAY:chararray);


filtro= FILTER vuelos by CANCELLED==1;
group_dias= GROUP filtro by DAY_OF_WEEK;
count_dias= FOREACH group_dias GENERATE group, COUNT($1) as n;

arrange_dias= ORDER count_dias by n DESC;
dump arrange_dias;

store arrange_dias INTO '$OUTPUT/ej3v' using PigStorage(',');