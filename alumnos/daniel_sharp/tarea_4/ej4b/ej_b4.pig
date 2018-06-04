-- Daniel Sharp 138176
-- Carga de los datos
flights = LOAD 's3://daniel-sharp/t4ej_b/inputs/flights.csv' USING PigStorage(',') AS (year,month,day,day_of_week:int,airline,flight_number,tail_number,origin_airport,destination_airport,
scheduled_departure,departure_time,departure_delay,taxi_out,wheels_off,scheduled_time,elapsed_time,
air_time,distance,wheels_on,taxi_in,scheduled_arrival,arrival_time,arrival_delay,diverted,cancelled:int,
cancellation_reason,air_system_delay,security_delay,airline_delay,late_aircraft_delay,weather_delay);

airports = load 's3://daniel-sharp/t4ej_b/inputs/airports.csv' using PigStorage(',') as (iata_code,airport,city,state,country,latitude,longitude);

-- Union de las tablas para obtener el nombre de los aeropuertos
joined = JOIN flights BY origin_airport, airports BY iata_code;

-- Agrupacion de los datos por aeropuerto
grouped = GROUP joined BY airport;

-- Calculo de las cancelaciones (dado que la variable de cancelled es booleana solo es necesario hacer la suma)
cancelled = FOREACH grouped GENERATE group as airport, SUM(joined.cancelled) as suma;

-- Selecciona unicamente los registros con 17 cancelaciones
filtered = FILTER cancelled BY suma == 17;

-- Seleccion de las columnas de interes
result = FOREACH filtered GENERATE airport, suma;

--Guardar los datos en S3
STORE result INTO 's3://daniel-sharp/t4ej_b/out_b4';


