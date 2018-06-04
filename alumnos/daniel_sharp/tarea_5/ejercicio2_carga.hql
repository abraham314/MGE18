drop database if exists flights cascade;

create database if not exists flights location "/usr/local/flights_db/";

create external table if not exists flights.airlines (
iata_code string,
airline string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/usr/local/flights_db/airlines'
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/usr/local/flights/airlines.csv' INTO table flights.airlines;

create external table if not exists flights.airports (
iata_code string,
airport string,
city string,
state string,
country string,
latitude double,
longitude double)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/usr/local/flights_db/airports'
tblproperties ("skip.header.line.count"="1");


LOAD DATA INPATH '/usr/local/flights/airports.csv'
INTO table flights.airports;

create external table if not exists flights.flightss (
year smallint,
month smallint,
day smallint,
day_of_week smallint,
airline string,
flight_number smallint,
tail_number string,
origin_airport string,
destination_airport string,
scheduled_departure smallint,
departure_time smallint,
departure_delay smallint,
taxi_out smallint,
wheels_off smallint,
scheduled_time smallint,
elapsed_time smallint,
air_time smallint,
distance smallint,
wheels_on smallint,
taxi_in smallint,
scheduled_arrival smallint,
arrival_time smallint,
arrival_delay smallint,
diverted smallint,
cancelled smallint,
cancellation_reason string,
air_system_delay string,
security_delay string,
airline_delay string,
late_aircraft_delay string,
weather_delay string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
location '/usr/local/flights_db/flightss'
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/usr/local/flights/flights.csv'
INTO table flights.flightss;

