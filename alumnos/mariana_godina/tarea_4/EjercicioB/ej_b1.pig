--ejercicio B
flights = load 's3://proyectopig/input/flights/flights.csv' using PigStorage(',') as (YEAR:int, MONTH:int, DAY:int, DAY_OF_WEEK:int, AIRLINE:chararray, FLIGHT_NUMBER:chararray, TAIL_NUMBER:chararray, ORIGIN_AIRPOR:chararray, DESTINATION_AIRPORT:chararray, SCHEDULED_DEPARTURE:int, DEPARTURE_TIME:int, DEPARTURE_DELAY:int, TAXI_OUT:int, WHEELS_OFF:int, SCHEDULED_TIME:int, ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, WHEELS_ON:int, TAXI_IN:int, SCHEDULED_ARRIVAL:int, ARRIVAL_TIME:int, ARRIVAL_DELAY:int, DIVERTED:int, CANCELLED:int);
airports = load 's3://proyectopig/input/flights/airports.csv' using PigStorage(',') as (IATA_CODE:chararray, AIRPORT:chararray, CITY:chararray, STATE:chararray, COUNTRY:chararray, LATITUDE:float, LONGITUDE:float);
by_dest = group flights by DESTINATION_AIRPORT;
count_flights = FOREACH by_dest GENERATE group as DESTINATION_AIRPORT,
COUNT(flights) as n;
flight_airports = join count_flights by DESTINATION_AIRPORT, airports by IATA_CODE;
vuelos_honolulu = FILTER flight_airports BY AIRPORT == 'Honolulu International Airport';
salida = FOREACH vuelos_honolulu GENERATE DESTINATION_AIRPORT, AIRPORT, n;
store salida into 's3://proyectopig/output/ejercicioB/p1';
