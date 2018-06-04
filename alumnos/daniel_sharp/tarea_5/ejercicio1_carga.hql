drop database if exists northwind cascade;

create database if not exists northwind location "s3://daniel-sharp/t5ej_1/northwind_db/";

create external table if not exists northwind.products (productid smallint,
productname string,
supplierid smallint,
categoryid smallint,
quantityperunit string,
unitprice float,
unitsinstock smallint,
unitsonorder smallint,
reorderlevel smallint,
discontinued int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://daniel-sharp/t5ej_1/northwind_db/products'
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://daniel-sharp/t5ej_1/inputs/products.csv' INTO table northwind.products;

create external table if not exists northwind.orders (
orderid smallint,
customerid string,
employeeid smallint,
orderdate timestamp,
requireddate timestamp,
shippeddate timestamp,
shipvia smallint,
freight float,
shipname string,
shipaddress string,
shipcity string,
shipregion string,
shippostalcode string,
shipcountry string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://daniel-sharp/t5ej_1/northwind_db/orders'
tblproperties ("skip.header.line.count"="1");


LOAD DATA INPATH 's3://daniel-sharp/t5ej_1/inputs/orders.csv'
INTO table northwind.orders;

create external table if not exists northwind.order_details (orderid smallint,
productid smallint,
unitprice float,
quantity smallint,
discount float)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
location 's3://daniel-sharp/t5ej_1/northwind_db/orderdetails'
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://daniel-sharp/t5ej_1/inputs/order_details.csv'
INTO table northwind.order_details;


create external table if not exists northwind.customers (
customerid string,
companyname string,
contactname string,
contacttitle string ,
address string,
city string,
region string,
postalcode string,
country string,
phone string,
fax string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
location 's3://daniel-sharp/t5ej_1/northwind_db/customers'
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://daniel-sharp/t5ej_1/inputs/customers.csv'
INTO table northwind.customers;

create external table if not exists northwind.employees (
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
photo string,
notes string,
reportsto smallint,
photopath string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
location 's3://daniel-sharp/t5ej_1/northwind_db/employees'
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://daniel-sharp/t5ej_1/inputs/employees.csv'
INTO table northwind.employees;
