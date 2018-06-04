drop database if exists flights cascade;

create database flights location "s3://audiracmichelle/hive/flights";

create external table if not exists flights.flights (
year int, month int, day int, day_of_week int, 
airline string, flight_number int, tail_number string,
origin_airport string, destination_airport string,
scheduled_departure int, departure_time int,
departure_delay int, taxi_out int, wheels_off int,
scheduled_time int, elapsed_time int, air_time int,
distance int, wheels_on int, taxi_in int, 
scheduled_arrival int, arrival_time int, arrival_delay int,
diverted int, cancelled int, cancellation_reason string,
air_system_delay string, security_delay string,
airline_delay string, late_aircraft_delay string,
weather_delay string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://audiracmichelle/hive/flights/flights'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH 's3://audiracmichelle/flights/flights.csv' 
INTO table flights.flights;

create external table if not exists flights.airports (
iata_code string, airport string, city string,
state string, country string,
latitude double, longitude double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://audiracmichelle/hive/flights/airports'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH 's3://audiracmichelle/flights/airports.csv'
INTO table flights.airports;

create external table if not exists flights.airlines (
iata_code string, airline string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://audiracmichelle/hive/flights/airlines'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH 's3://audiracmichelle/flights/airlines.csv'
INTO table flights.airlines;