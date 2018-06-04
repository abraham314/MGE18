-- Daniel Sharp 138176
-- Carga de las tablas
flights = load 's3://daniel-sharp/t4ej_b/inputs/flights.csv' using PigStorage(',') as (year,month,day,day_of_week,airline,flight_number,tail_number,origin_airport,destination_airport,
scheduled_departure,departure_time,departure_delay,taxi_out,wheels_off,scheduled_time,elapsed_time,
air_time,distance,wheels_on,taxi_in,scheduled_arrival,arrival_time,arrival_delay,diverted,cancelled,
cancellation_reason,air_system_delay,security_delay,airline_delay,late_aircraft_delay,weather_delay);

airports = load 's3://daniel-sharp/t4ej_b/inputs/airports.csv' using PigStorage(',') as (iata_code,airport,city,state,country,latitude,longitude);

-- Join entre las tablas a traves del codigo de aeropuerto para obtener su nombre
flights_airname = join flights by destination_airport, airports by iata_code; 

-- Filtramos las observaciones para tomar solamente las de Honolulu
filtered = FILTER flights_airname BY airport == 'Honolulu International Airport';

-- Agrupamos las observaciones para poder tomar el COUNT
grouped = group filtered all;

-- Tomamos el COUNT de registros
count = foreach grouped generate COUNT(filtered) as n;

-- Guardamos el resultado en S3
store count into 's3://daniel-sharp/t4ej_b/out_b1';


