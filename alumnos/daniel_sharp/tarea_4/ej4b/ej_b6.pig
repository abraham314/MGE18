-- Daniel Sharp 138176
-- Carga de las tablas
flights = LOAD 's3://daniel-sharp/t4ej_b/inputs/flights.csv' USING PigStorage(',') AS (year,month,day,day_of_week:int,airline,flight_number,tail_number,origin_airport,destination_airport,
scheduled_departure,departure_time,departure_delay,taxi_out,wheels_off,scheduled_time,elapsed_time,
air_time,distance,wheels_on,taxi_in,scheduled_arrival,arrival_time,arrival_delay,diverted,cancelled:int,
cancellation_reason,air_system_delay,security_delay,airline_delay,late_aircraft_delay,weather_delay);

airports = LOAD 's3://daniel-sharp/t4ej_b/inputs/airports.csv' USING PigStorage(',') AS (iata_code,airport,city,state,country,latitude,longitude);

-- Union entre tablas para obtener el nombre del aeropuerto destino
joined = JOIN flights BY destination_airport, airports BY iata_code;

-- Agrupar datos por numero de vuelo
grouped = GROUP joined BY flight_number;

-- Para cada numero de vuelo se obtienen los distintos aeropuertos que hay y se obtiene la cuenta de aeropuertos distintos que hay por numero de vuelo junto con la lista completa de ellos
counted = FOREACH grouped { unique_airports = DISTINCT joined.airport; GENERATE group as flight_number, COUNT(unique_airports) as destinations, unique_airports;};

-- Se ordenan los registros de acuerdo al numero de distintos aeropuertos destino de cada vuelo
ordered = ORDER counted BY destinations DESC;

-- Se seleccionan los primeros 5 registros
limited = LIMIT ordered 5;

-- Se guardan resultados en S3
STORE limited INTO 's3://daniel-sharp/t4ej_b/out_b6';


