products = LOAD 's3://metodosgranescala/northwind/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);
order_details = LOAD 's3://metodosgranescala/northwind/order_details.csv'using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);
group_orders = GROUP order_details BY productid;
count_products = FOREACH group_orders GENERATE group as productid , COUNT($1) as n;
ranked = rank count_products by n DESC;
-- El mejor rank
limited_rank = limit ranked 1; 
join_limited_product = JOIN limited_rank BY productid, products BY productid;
pregunta_a = FOREACH join_limited_product GENERATE $1 as productid, productname,n; 
STORE pregunta_a INTO 's3://metodosgranescala/output/northwind/salida'  USING PigStorage(',');
