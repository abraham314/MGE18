DROP DATABASE IF EXISTS northwind CASCADE;

CREATE DATABASE IF NOT EXISTS northwind 
LOCATION 's3://mge-itam-2018/tarea5/hive/northwind';

CREATE EXTERNAL TABLE IF NOT EXISTS northwind.products(
productid smallint,
productname string,
supplierid smallint,
categoryid smallint,
quantityperunit string,
unitprice float,
unitsinstock smallint,
unitsonorder smallint,
reorderlevel smallint,
discontinued int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://mge-itam-2018/tarea5/hive/northwind/products';

LOAD DATA INPATH 's3://mge-itam-2018/tarea4/northwind/products.csv' INTO TABLE northwind.products;

CREATE EXTERNAL TABLE IF NOT EXISTS northwind.orders (
orderid smallint,
customerid string,
employeeid smallint,
orderdate string,
requireddate string,
shippeddate string,
shipvia smallint,
freight float,
shipname string,
shipaddress string,
shipcity string,
shipregion string,
shippostalcode string,
shipcountry string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://mge-itam-2018/tarea5/hive/northwind/orders'
TBLPROPERTIES ("skip.header.line.count"="1");


LOAD DATA INPATH 's3://mge-itam-2018/tarea4/northwind/orders.csv'
INTO TABLE northwind.orders;

CREATE EXTERNAL TABLE IF NOT EXISTS northwind.order_details (orderid smallint,
productid smallint,
unitprice float,
quantity smallint,
discount float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://mge-itam-2018/tarea5/hive/northwind/orderdetails';

LOAD DATA INPATH 's3://mge-itam-2018/tarea4/northwind/order_details.csv'
INTO TABLE northwind.order_details;

CREATE EXTERNAL TABLE IF NOT EXISTS northwind.employees (
employeeid smallint,
lastname string,
firstname string,
title string,
titleofcourtesy string,
birthdate string,
hiredate string,
address string,
city string,
region string,
postalcode string,
country string,
homephone string,
extension string,
photo BINARY,
notes string,
reportsto Array<int>,
photopath string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://mge-itam-2018/tarea5/hive/northwind/employees'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://mge-itam-2018/tarea4/northwind/employees.csv'
INTO TABLE northwind.employees;

CREATE EXTERNAL TABLE IF NOT EXISTS northwind.customers (
customerid smallint,
companyname string,
contactname string,
contacttitle string,
address string,
city string,
region string,
postalcode string,
country string,
phone string,
fax string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://mge-itam-2018/tarea5/hive/northwind/customers'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://mge-itam-2018/tarea4/northwind/customers.csv'
INTO TABLE northwind.customers;

DROP DATABASE IF EXISTS flights CASCADE;

CREATE DATABASE IF NOT EXISTS flights location "s3://mge-itam-2018/tarea5/hive/flights";

CREATE EXTERNAL TABLE IF NOT EXISTS flights.airlines (
iata_code string,
airline string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://mge-itam-2018/tarea5/hive/flights/airlines'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://mge-itam-2018/tarea4/flights/airlines.csv' INTO TABLE flights.airlines;

CREATE EXTERNAL TABLE IF NOT EXISTS flights.airports (
iata_code string,
airport string,
city string,
state string,
country string,
latitude double,
longitude double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://mge-itam-2018/tarea5/hive/flights/airports'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://mge-itam-2018/tarea4/flights/airports.csv'
INTO TABLE flights.airports;

CREATE EXTERNAL TABLE IF NOT EXISTS flights.flights (
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
LOCATION 's3://mge-itam-2018/tarea5/hive/flights/flights'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://mge-itam-2018/tarea4/flights/flights.csv'
INTO TABLE flights.flights;
