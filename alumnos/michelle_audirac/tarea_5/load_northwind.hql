drop database if exists northwind cascade;

create database if not exists northwind location "s3://audiracmichelle/hive/northwind/";

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
LOCATION 's3://audiracmichelle/hive/northwind/products'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH 's3://audiracmichelle/northwind/products.csv' INTO table northwind.products;

create external table if not exists northwind.orders (
orderid smallint,
customerid string,
employeeid smallint,
orderdate string,
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://audiracmichelle/hive/northwind/orders'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH 's3://audiracmichelle/northwind/orders.csv'
INTO table northwind.orders;

create external table if not exists northwind.order_details (orderid smallint,
productid smallint,
unitprice float,
quantity smallint,
discount float)
row format delimited fields terminated by ','
location 's3://audiracmichelle/hive/northwind/orderdetails'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH 's3://audiracmichelle/northwind/order_details.csv'
INTO table northwind.order_details;

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
location 's3://audiracmichelle/hive/employees'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH 's3://audiracmichelle/northwind/employees.csv'
INTO table northwind.employees;

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
location 's3://audiracmichelle/hive/customers'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH 's3://audiracmichelle/northwind/customers.csv'
INTO table northwind.customers;
