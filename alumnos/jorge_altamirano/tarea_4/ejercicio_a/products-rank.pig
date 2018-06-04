--- Apache Pig Script to load flights data into Pig
--- Author: Jorge III Altamirano Astorga
--- 
--- Reference: http://d2vlcm61l7u1fs.cloudfront.net/media%2F5a6%2F5a6dbbe9-db6e-448b-91ed-8126085eec67%2FphpqsVSNv.png
customers = load '/data/northwind/customers.csv' using PigStorage(',') as (
  customerid:chararray, companyname:chararray, contactname:chararray,
  contacttitle:chararray, address:chararray, city:chararray, 
  region:chararray, postalcode:chararray, country:chararray,
  phone:chararray, fax:chararray
);
employees = load '/data/northwind/employees.csv' using PigStorage(',') as (
  employeeid:int, lastname:chararray, firstname:chararray,
  title:chararray, titleofcourtesy:chararray, birthdate:chararray,
  hiredate:chararray, address:chararray, city:chararray, 
  region:chararray, postalcode:chararray, country:chararray, 
  homephone:chararray, extension:chararray, photo:chararray, 
  notes:chararray, reportsto:chararray, photopath:chararray
);
orderdetails = load '/data/northwind/order_details.csv' using PigStorage(',') as (
  orderid:int, productid:int, unitprice:float, quantity:int,
  discount:float
);
--- didn't implement as datetime due to data loss prevention :-)
orders = load '/data/northwind/orders.csv' using PigStorage(',') as (
  orderid:int, customerid:chararray, employeeid:int,
  orderdate:chararray, requireddate:chararray, shippeddate:chararray, 
  shipvia:int, freight:float, shipname:chararray,
  shipaddress:chararray, shipcity:chararray, shipregion:chararray,
  shippostalcode:chararray, shipcountry:chararray
);
products = load '/data/northwind/products.csv' using PigStorage(',') as (
  productid:int, productname:chararray, supplierid:int,
  categoryid:int, quantityperunit:chararray, unitprice:float,
  unitsinstock:int, unitsonorder:int, reorderlevel:int,
  discontinued:boolean
);

group_orders = group orderdetails by productid;
count_products = FOREACH group_orders GENERATE group as productid, COUNT($1) as n;
ranked = rank count_products by n DESC;
limited_rank = limit ranked 1;
out = JOIN products by productid, limited_rank by productid;
store out into '/tarea_4/ejercicio_a' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

