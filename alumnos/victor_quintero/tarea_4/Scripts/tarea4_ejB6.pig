flights = load '$INPUT/flights.csv' using PigStorage(',') as (YEAR:int,MONTH:int,DAY:int,DAY_OF_WEEK:int,AIRLINE:chararray,FLIGHT_NUMBER:int,TAIL_NUMBER:chararray,ORIGIN_AIRPORT:chararray,DESTINATION_AIRPORT:chararray,SCHEDULED_DEPARTURE:chararray,DEPARTURE_TIME:chararray,DEPARTURE_DELAY:int,TAXI_OUT:int,WHEELS_OFF:chararray,SCHEDULED_TIME:chararray,ELAPSED_TIME:int,AIR_TIME:int,DISTANCE:int,WHEELS_ON:chararray,TAXI_IN:int,SCHEDULED_ARRIVAL:chararray,ARRIVAL_TIME:chararray,ARRIVAL_DELAY:int,DIVERTED:chararray,CANCELLED:chararray,CANCELLATION_REASON:chararray,AIR_SYSTEM_DELAY:chararray,SECURITY_DELAY:chararray,AIRLINE_DELAY:chararray,LATE_AIRCRAFT_DELAY:chararray,WEATHER_DELAY:chararray);

airports = load '$INPUT/airports.csv' using PigStorage(',') as (IATA_CODE:chararray,AIRPORT:chararray,CITY:chararray,STATE:chararray,COUNTRY:chararray,LATITUDE:float,LONGITUDE:float);

flights_airports = JOIN flights by DESTINATION_AIRPORT LEFT OUTER, airports by IATA_CODE; 
flights_group = GROUP flights_airports BY FLIGHT_NUMBER;
air_unique = FOREACH flights_group{dest = FOREACH flights_airports GENERATE DESTINATION_AIRPORT; dist = DISTINCT dest; GENERATE group as FLIGHT_NUMBER, COUNT(dist) as n_unique;};
destinations_order = ORDER air_unique by n_unique DESC;
top = limit destinations_order 1;

flights_destination = FOREACH flights GENERATE FLIGHT_NUMBER, DESTINATION_AIRPORT;
airport_names = FOREACH airports GENERATE IATA_CODE, AIRPORT;
dest_uniq = distinct flights_destination;
resumen = join top by FLIGHT_NUMBER, dest_uniq by FLIGHT_NUMBER;
final1 = join resumen by DESTINATION_AIRPORT LEFT OUTER, airport_names by IATA_CODE;
final = FOREACH final1 GENERATE $0 as flight_number, $1 as diff_dest, $3 as iata_code, $5 as airport_name;



store final into '$OUTPUT/output6/' using PigStorage(',', '-schema');
