--Daniel Sharp 138176
-- Carga de los datos
flights = LOAD 's3://daniel-sharp/t4ej_b/inputs/flights.csv' USING PigStorage(',') AS (year,month,day,day_of_week:int,airline,flight_number,tail_number,origin_airport,destination_airport,
scheduled_departure,departure_time,departure_delay,taxi_out,wheels_off,scheduled_time,elapsed_time,
air_time,distance,wheels_on,taxi_in,scheduled_arrival,arrival_time,arrival_delay,diverted,cancelled:int,
cancellation_reason,air_system_delay,security_delay,airline_delay,late_aircraft_delay,weather_delay);

airports = LOAD 's3://daniel-sharp/t4ej_b/inputs/airports.csv' USING PigStorage(',') AS (iata_code,airport,city,state,country,latitude,longitude);

-- Union de las tablas para obtener el nombre del aeropuerto de origen
joined = JOIN flights BY origin_airport, airports BY iata_code;

-- Agrupacion de los datos por aeropuerto
grouped = GROUP joined BY airport;

-- Suma de los vuelos cancelados por aeropuerto
cancelled = FOREACH grouped GENERATE group as airport, SUM(joined.cancelled) as suma;

-- Seleccion de variables de interes
selected = FOREACH cancelled GENERATE airport, suma;

-- Ordenamiento de los datos de acuerdo al numero de cancelaciones
ordered = ORDER selected BY suma DESC;

-- Seleccion de las primeras 5
limited = LIMIT ordered 5;

-- Guarda datos en S3
STORE limited INTO 's3://daniel-sharp/t4ej_b/out_b5';


