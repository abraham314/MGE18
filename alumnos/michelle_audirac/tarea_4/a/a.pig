order_details = load 's3://audiracmichelle/northwind/order_details.csv' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);
order_details = foreach order_details generate productid;

products = load 's3://audiracmichelle/northwind/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);
products = foreach products generate ..productname;

order_details = JOIN order_details by productid, products by productid;

group_products = group order_details by $2;
count_products = FOREACH group_products GENERATE $0 as productname, COUNT($1) as n;

ranked = rank count_products by n DESC;
ranked = limit ranked 10;

store ranked into 's3://audiracmichelle/a/output/' using PigStorage(',');