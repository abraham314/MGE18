/******************  Ejercicio A   ************************/
/******************  Cargamos datasets de s3 **************/
products = load 's3://al102964-bucket1/tarea4/ejercicio_a/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);
orders = load 's3://al102964-bucket1/tarea4/ejercicio_a/order_details.csv' using PigStorage(',') as (orderid:chararray,productid:chararray,unitprice:float,quantity:int,	discount:float);

/*Hacemos operaciones*/
group_orders = group orders by productid;
count_products = FOREACH group_orders GENERATE group as productid,COUNT($1) as n;
join_products_orders = JOIN count_products by productid, products by productid;
ranked = rank join_products_orders by n DESC;
limited_rank = limit ranked 10;
output_data = FOREACH limited_rank generate $0 as ranking, $1 as pid, $4 as name, $2 as total_orders;

/*Escribimos a s3*/
store output_data into 's3://al102964-bucket1/tarea4/ejercicio_a/output/' using PigStorage(',', '-schema');