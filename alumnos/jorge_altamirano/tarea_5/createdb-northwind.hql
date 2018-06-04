DROP DATABASE IF EXISTS northwind CASCADE;

create database northwind location "s3://jorge-altamirano/hive/northwind";

use northwind;

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
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://jorge-altamirano/hive/northwindproducts'
tblproperties ("skip.header.line.count"="1");

create external table if not exists northwind.orders (
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
LOCATION 's3://jorge-altamirano/hive/northwindorders'
tblproperties ("skip.header.line.count"="1");

create external table if not exists northwind.orders (
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
LOCATION 's3://jorge-altamirano/hive/northwindorders'
tblproperties ("skip.header.line.count"="1");

create external table if not exists northwind.employees (employeeid smallint,
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
reportsto string,
photopath string)
row format delimited fields terminated by ','
location 's3://jorge-altamirano/hive/northwindemployees'
tblproperties ("skip.header.line.count"="1");

create external table if not exists northwind.orderdetails (orderid smallint,
productid smallint,
unitprice float,
quantity smallint,
discount float)
row format delimited fields terminated by ','
location 's3://jorge-altamirano/hive/northwindorderdetails'
tblproperties ("skip.header.line.count"="1");

create external table if not exists northwind.customers (customerid string,
companyname string,
contactname string,
contacttitle string,
address string,
city string,
region string,
postalcode string,
country string,
phone string,
fax string)
row format delimited fields terminated by ','
location 's3://jorge-altamirano/hive/northwindcustomers'
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://jorge-altamirano/northwind/products.csv' 
INTO table northwind.products;

LOAD DATA INPATH 's3://jorge-altamirano/northwind/orders.csv'
INTO table northwind.orders;

LOAD DATA INPATH 's3://jorge-altamirano/northwind/order_details.csv'
INTO table northwind.orderdetails;

LOAD DATA INPATH 's3://jorge-altamirano/northwind/employees.csv'
INTO table northwind.employees;

LOAD DATA INPATH 's3://jorge-altamirano/northwind/customers.csv'
INTO table northwind.customers;

