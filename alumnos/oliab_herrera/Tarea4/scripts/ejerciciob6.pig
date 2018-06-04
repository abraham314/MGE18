
vuelos = load '$INPUT/flights' using PigStorage(',') 
as (YEAR:chararray, MONTH:chararray, DAY:chararray, DAY_OF_WEEK:chararray,AIRLINE:chararray,
    FLIGHT_NUMBER:chararray,TAIL_NUMBER:chararray,ORIGIN_AIRPORT:chararray,DESTINATION_AIRPORT:chararray,
    SCHEDULED_DEPARTURE:int, DEPARTURE_TIME:int, DEPARTURE_DELAY:int,TAXI_OUT: int,WHEELS_OFF:chararray,SCHEDULED_TIME:chararray,
    ELAPSED_TIME:chararray,AIR_TIME:int, DISTANCE: int, WHEELS_ON:chararray, TAXI_IN:int, SCHEDULED_ARRIVAL:chararray, 
    ARRIVAL_TIME:chararray,ARRIVAL_DELAY:int,DIVERTED:chararray,CANCELLED:int,CANCELLATION_REASON:chararray,AIR_SYSTEM_DELAY:chararray,
    SECURITY_DELAY:chararray,AIRLINE_DELAY:chararray,LATE_AIRCRAFT_DELAY:chararray,WEATHER_DELAY:chararray);

aeropuertos = load '$INPUT/airports' using PigStorage(',') as (IATA_CODE:chararray, AIRPORT:chararray, CITY:chararray, STATE:chararray, COUNTRY:chararray, LATITUDE:float, LONGITUDE:float);

flights= FOREACH vuelos generate FLIGHT_NUMBER, DESTINATION_AIRPORT;
aero = FOREACH aeropuertos generate IATA_CODE,AIRPORT;
group_vuelo = group flights by FLIGHT_NUMBER;
aero_unico  = FOREACH group_vuelo {
               dest = flights.DESTINATION_AIRPORT;
               unico = distinct dest;
               generate group as flight_number, COUNT(unico) as n;
};
ranked = rank aero_unico by n DESC;
ranked1 = limit ranked 1;
vuelo_dist = distinct flights;
join_vuelos = join ranked1 by flight_number, vuelo_dist by FLIGHT_NUMBER; 

store join_vuelos into '$OUTPUT/ej6' using PigStorage(',');



