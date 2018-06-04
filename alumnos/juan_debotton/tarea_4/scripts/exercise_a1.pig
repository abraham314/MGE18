order_details = LOAD 's3://mge-itam-2018/tarea4/northwind/order_details.csv' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);

product_order_details = FOREACH order_details GENERATE productid;

products = LOAD 's3://mge-itam-2018/tarea4/northwind/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);

product_names = FOREACH products GENERATE productid, productname;

product_join_orders = JOIN product_order_details BY productid, product_names BY productid;

product_join_orders_clean = FOREACH product_join_orders GENERATE $1, $2;

group_products = GROUP product_join_orders_clean BY productname;

count_products = FOREACH group_products GENERATE $0 AS product_names, COUNT($1) as n;

ranked = rank count_products by n DESC;

top_10 = limit ranked 10;

STORE top_10 into 's3://mge-itam-2018/tarea4/northwind/output' USING PigStorage(',');