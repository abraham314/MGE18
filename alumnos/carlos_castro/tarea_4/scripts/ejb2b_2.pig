rmf $OUTPUT

flights = load '$INPUT/flights.csv' using PigStorage(',') as (YEAR:int, MONTH:int, DAY:int, DAY_OF_WEEK:int, AIRLINE:chararray , FLIGHT_NUMBER:int, TAIL_NUMBER:chararray , ORIGIN_AIRPORT:chararray , DESTINATION_AIRPORT:chararray , SCHEDULED_DEPARTURE:chararray , DEPARTURE_TIME:chararray , DEPARTURE_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray , SCHEDULED_TIME:int, ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, WHEELS_ON:chararray , TAXI_IN:int, SCHEDULED_ARRIVAL:chararray , ARRIVAL_TIME:chararray , ARRIVAL_DELAY:int, DIVERTED:int, CANCELLED:int, CANCELLATION_REASON:chararray , AIR_SYSTEM_DELAY:int, SECURITY_DELAY:int, AIRLINE_DELAY:int, LATE_AIRCRAFT_DELAY:int, WEATHER_DELAY:int);

B = GROUP flights BY FLIGHT_NUMBER;
X = FOREACH B GENERATE group, MAX(flights.ARRIVAL_DELAY) as n;
ranked = rank X by n DESC;
limited_rank = limit ranked 1;
store limited_rank into '$OUTPUT';
