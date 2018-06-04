products = LOAD 's3://granescala-t4/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);

order_details = LOAD 's3://granescala-t4/order_details.csv' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);

group_d = GROUP order_details BY productid;

conteo = FOREACH group_d GENERATE group as productid , COUNT($1) as n;

rankeo = rank conteo by n DESC;

limrank = limit rankeo 10;


joinprod = JOIN limrank BY productid, products BY productid;

res = FOREACH joinprod GENERATE $1 as productid, productname, n;

STORE res INTO 's3://granescala-t4/resultadoejercicioA.csv' using PigStorage(',');
