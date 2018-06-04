DROP DATABASE IF EXISTS flights CASCADE;

create database flights location "s3://jorge-altamirano/hive/flights";

use flights;

create external table if not exists flights.flights (
  year int, month int, day int, day_of_week int, 
  airline string, flight_number int, tail_number string,
  origin_airport string, destination_airport string,
  scheduled_departure int, departure_time int,
  departure_delay bigint, taxi_out int, wheels_off int,
  scheduled_time int, elapsed_time int, air_time int,
  distance int, wheels_on int, taxi_in int, 
  scheduled_arrival int, arrival_time int, arrival_delay int,
  diverted int, cancelled int, cancellation_reason string,
  air_system_delay string, security_delay string,
  airline_delay string, late_aircraft_delay string,
  weather_delay string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://jorge-altamirano/hive/flightsflights'
tblproperties ("skip.header.line.count"="1");

create external table if not exists flights.airports (
  iata_code string, airport string, city string,
  state string, country string,
  latitude double, longitude double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://jorge-altamirano/hive/flightsairports'
tblproperties ("skip.header.line.count"="1");

create external table if not exists flights.airlines (
  iata_code string, airline string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://jorge-altamirano/hive/flightsairlines'
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://jorge-altamirano/flights/flights.csv' 
INTO table flights.flights;

LOAD DATA INPATH 's3://jorge-altamirano/flights/airports.csv'
INTO table flights.airports;

LOAD DATA INPATH 's3://jorge-altamirano/flights/airlines.csv'
INTO table flights.airlines;
