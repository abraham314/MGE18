drop database if exists flights cascade;

create database if not exists flights location "s3://hive-metodos2/database/flights";

create external table if not exists flights.airlines (
iata_code string,
airline string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://hive-metodos2/database/flights/airlines'
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH 's3://hive-metodos2/Data/airlines.csv' INTO table flights.airlines;

create external table if not exists flights.airports (
iata_code string,
airport string,
city string,
state string,
country string,
latitude double,
longitude double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://hive-metodos2/database/flights/airports'
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH 's3://hive-metodos2/Data/airports.csv' INTO table flights.airports;

create external table if not exists flights.flights (
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://hive-metodos2/database/flights/flights'
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH 's3://hive-metodos2/Data/flights.csv' INTO table flights.flights;
