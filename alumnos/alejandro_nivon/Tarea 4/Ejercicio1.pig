products = LOAD 's3://granescala-t4/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);

orders = LOAD 's3://granescala-t4/order_details.csv' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);

groupos = GROUP orders BY productid;

conteo_productos = FOREACH groupos GENERATE group as productid , COUNT($1) as n;

ranked = rank conteo_productos by n DESC;

lim_rank = limit ranked 10;

join_product = JOIN lim_rank BY productid, products BY productid;

result = FOREACH join_product GENERATE $1 as productid, productname, n;

STORE result INTO 's3://granescala-t4/resultadoejercicio1.csv' USING PigStorage(',');
