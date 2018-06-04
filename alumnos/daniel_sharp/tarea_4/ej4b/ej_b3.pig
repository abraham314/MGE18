--Daniel Sharp 138176
-- Carga de la tabla de datos
flights = LOAD 's3://daniel-sharp/t4ej_b/inputs/flights.csv' USING PigStorage(',') AS (year,month,day,day_of_week:int,airline,flight_number,tail_number,origin_airport,destination_airport,
scheduled_departure,departure_time,departure_delay,taxi_out,wheels_off,scheduled_time,elapsed_time,
air_time,distance,wheels_on,taxi_in,scheduled_arrival,arrival_time,arrival_delay,diverted,cancelled:int,
cancellation_reason,air_system_delay,security_delay,airline_delay,late_aircraft_delay,weather_delay);

-- Seleccion de las columnas de interes
selected = FOREACH flights GENERATE day_of_week, cancelled;

-- Agrupamos los datos por dia de la semana (va del 1 al 7)
grouped = GROUP selected BY day_of_week;

-- Resumimos los datos con la suma de vuelos cancelados para cada día
cancelled = FOREACH grouped GENERATE group as day_of_week, SUM(selected.cancelled) as suma;

-- Guardamos resultado en S3
STORE cancelled INTO 's3://daniel-sharp/t4ej_b/out_b3';


