--ejercicio B2
flights = load 's3://proyectopig/input/flights/flights.csv' using PigStorage(',') as (YEAR:int, MONTH:int, DAY:int, DAY_OF_WEEK:int, AIRLINE:chararray, FLIGHT_NUMBER:chararray, TAIL_NUMBER:chararray, ORIGIN_AIRPOR:chararray, DESTINATION_AIRPORT:chararray, SCHEDULED_DEPARTURE:int, DEPARTURE_TIME:int, DEPARTURE_DELAY:int, TAXI_OUT:int, WHEELS_OFF:int, SCHEDULED_TIME:int, ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, WHEELS_ON:int, TAXI_IN:int, SCHEDULED_ARRIVAL:int, ARRIVAL_TIME:int, ARRIVAL_DELAY:int, DIVERTED:int, CANCELLED:int);
airlines = load 's3://proyectopig/input/flights/airlines.csv' using PigStorage(',') as (IATA_CODE:chararray, AIRLINE:chararray);
flights_sel = FOREACH flights GENERATE FLIGHT_NUMBER, AIRLINE, ARRIVAL_DELAY;
flight_airlines = join flights_sel by AIRLINE, airlines by IATA_CODE;
delay_max = ORDER flight_airlines BY ARRIVAL_DELAY DESC;
max_air = limit delay_max 1;
store max_air into 's3://proyectopig/output/ejercicioB/p2' USING PigStorage(',');
