--Daniel Sharp 138176
-- Carga de las tablas
flights = LOAD 's3://daniel-sharp/t4ej_b/inputs/flights.csv' USING PigStorage(',') AS (year,month,day,day_of_week,airline,flight_number,tail_number,origin_airport,destination_airport,
scheduled_departure,departure_time,departure_delay,taxi_out,wheels_off,scheduled_time,elapsed_time,
air_time,distance,wheels_on,taxi_in,scheduled_arrival,arrival_time,arrival_delay,diverted,cancelled,
cancellation_reason,air_system_delay,security_delay,airline_delay,late_aircraft_delay,weather_delay);

airlines = LOAD 's3://daniel-sharp/t4ej_b/inputs/airlines.csv' USING PigStorage(',') AS (iata_code,airline_name);

--Union de las tablas a traves del codigo de aerolinea para obtener el nombre de la aerolinea
flights_airname = JOIN flights BY airline, airlines BY iata_code; 

-- Ordenamos los datos de manera descendente de acuerdo a su retraso de arribo
ordered = ORDER flights_airname BY arrival_delay DESC;

-- Seleccionamos únicamente las columnas de interés
selected = FOREACH ordered GENERATE flight_number, tail_number, airline_name, arrival_delay;

-- Limitamos a las primeras 5 observaciones
limited = LIMIT selected 5;

-- Guardamos resultado en s3
store limited into 's3://daniel-sharp/t4ej_b/out_b2';


