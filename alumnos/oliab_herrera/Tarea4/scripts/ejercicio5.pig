

vuelos = load '$INPUT/flights' using PigStorage(',') 
as (YEAR:chararray, MONTH:chararray, DAY:chararray, DAY_OF_WEEK:chararray,AIRLINE:chararray,
    FLIGHT_NUMBER:chararray,TAIL_NUMBER:chararray,ORIGIN_AIRPORT:chararray,DESTINATION_AIRPORT:chararray,
    SCHEDULED_DEPARTURE:int, DEPARTURE_TIME:int, DEPARTURE_DELAY:int,TAXI_OUT: int,WHEELS_OFF:chararray,SCHEDULED_TIME:chararray,
    ELAPSED_TIME:chararray,AIR_TIME:int, DISTANCE: int, WHEELS_ON:chararray, TAXI_IN:int, SCHEDULED_ARRIVAL:chararray, 
    ARRIVAL_TIME:chararray,ARRIVAL_DELAY:int,DIVERTED:chararray,CANCELLED:int,CANCELLATION_REASON:chararray,AIR_SYSTEM_DELAY:chararray,
    SECURITY_DELAY:chararray,AIRLINE_DELAY:chararray,LATE_AIRCRAFT_DELAY:chararray,WEATHER_DELAY:chararray);

aeropuertos = load '$INPUT/airports' using PigStorage(',') as (IATA_CODE:chararray, AIRPORT:chararray, CITY:chararray, STATE:chararray, COUNTRY:chararray, LATITUDE:float, LONGITUDE:float);

filtro = FILTER vuelos by CANCELLED==1;

group_air = GROUP filtro by ORIGIN_AIRPORT;
count_air = FOREACH group_air GENERATE group as airport, COUNT($1) as n;


join_rank = JOIN count_air by airport, aeropuertos by IATA_CODE;

rank_j = RANK join_rank by n DESC;

top = limit rank_j 5;


store top INTO '$OUTPUT/ej5' using PigStorage(',');