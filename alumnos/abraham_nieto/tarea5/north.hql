drop database if exists northwind cascade;

create database if not exists northwind location "s3://tarea5/northwind/";

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
LOCATION 's3://tarea5/northwind/products';

LOAD DATA INPATH 's3://tarea5/products.csv' INTO table northwind.products;

create external table if not exists northwind.orders (
orderid smallint,
customerid string,
employeeid smallint,
orderdate date,
requireddate date,
shippeddate date,
shipvia smallint,
freight float,
shipname string,
shipaddress string,
shipcity string,
shipregion string,
shippostalcode string,
shipcountry string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://tarea5/northwind/orders';


LOAD DATA INPATH 's3://tarea5/orders.csv'
INTO table northwind.orders;

create external table if not exists northwind.order_details (orderid smallint,
productid smallint,
unitprice float,
quantity smallint,
discount float)
row format delimited fields terminated by ','
location 's3://tarea5/northwind/orderdetails';

LOAD DATA INPATH 's3://tarea5/order_details.csv'
INTO table northwind.order_details;



create external table if not exists northwind.employees (employeeid smallint,
lastname string,
firstname string,
title string,
titleofcourtesy string,
birthdate date,
hiredate date,
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
row format delimited fields terminated by ','
location 's3://tarea5/northwind/employees';

LOAD DATA INPATH 's3://tarea5/employees.csv'
INTO table northwind.employees;

