products = load 's3://mat34710/tarea4/datos/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);
order_details = load 's3://mat34710/tarea4/datos/order_details.csv' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);
group_orders = group order_details by productid;
count_products = FOREACH group_orders GENERATE group as productid, COUNT($1) as n;
ranked = rank count_products by n DESC;
limited_rank = limit ranked 1;
resultado = JOIN limited_rank by productid, products by productid;
respuesta = FOREACH resultado generate $1 as productid, productname as productname, n as n_orders;
STORE respuesta INTO 's3://mat34710/tarea4/outputs/preguntaA' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
