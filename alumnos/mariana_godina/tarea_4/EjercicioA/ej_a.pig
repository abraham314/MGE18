--ejercicio A
products = load 's3://proyectopig/input/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);
order_details = load 's3://proyectopig/input/order_details.csv' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);
products_orders = join order_details by productid, products by productid;
group_orders = group products_orders by productname;
count_products = FOREACH group_orders GENERATE group as productname,
COUNT(products_orders) as n;
ranked = rank count_products by n DESC;
limited_rank = limit ranked 1;
store limited_rank into 's3://proyectopig/output/ejercicioA' USING PigStorage(',');
