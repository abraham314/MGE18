flights = load '$INPUT/flights.csv' using PigStorage(',') as (YEAR:int,MONTH:int,DAY:int,DAY_OF_WEEK:int,AIRLINE:chararray,FLIGHT_NUMBER:int,TAIL_NUMBER:chararray,ORIGIN_AIRPORT:chararray,DESTINATION_AIRPORT:chararray,SCHEDULED_DEPARTURE:chararray,DEPARTURE_TIME:chararray,DEPARTURE_DELAY:int,TAXI_OUT:int,WHEELS_OFF:chararray,SCHEDULED_TIME:chararray,ELAPSED_TIME:int,AIR_TIME:int,DISTANCE:int,WHEELS_ON:chararray,TAXI_IN:int,SCHEDULED_ARRIVAL:chararray,ARRIVAL_TIME:chararray,ARRIVAL_DELAY:int,DIVERTED:chararray,CANCELLED:chararray,CANCELLATION_REASON:chararray,AIR_SYSTEM_DELAY:chararray,SECURITY_DELAY:chararray,AIRLINE_DELAY:chararray,LATE_AIRCRAFT_DELAY:chararray,WEATHER_DELAY:chararray);
airlines =load '$INPUT/airlines.csv' using PigStorage(',') as (IATA_CODE:chararray,AIRLINE_2:chararray);

flights_airlines = JOIN flights by AIRLINE, airlines by IATA_CODE;

flights_ord = ORDER flights_airlines BY ARRIVAL_DELAY DESC;

top_delay = LIMIT flights_ord 1;

final = FOREACH top_delay GENERATE FLIGHT_NUMBER, TAIL_NUMBER,  ARRIVAL_DELAY,AIRLINE_2,IATA_CODE;

store final into '$OUTPUT/output2/'  using PigStorage(',', '-schema');
