/* *EJERCICIO A* */

/* Datos */

products = load 's3://driflore94maestria/data/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);
order_details = load 's3://driflore94maestria/data/order_details.csv' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);



/* Codigo */

group_orders = group order_details by productid;
count_products = FOREACH group_orders GENERATE group as productid,COUNT($1) as n;
join_products_orders = JOIN count_products by productid, products by productid;
ranked = rank join_products_orders by n DESC;
limited_rank = limit ranked 10;
result_data = FOREACH limited_rank generate $0 as ranking, $4 as product_name, $2 as total_orders;
store result_data into 's3://driflore94maestria/ej1a' using PigStorage(',', '-schema');
