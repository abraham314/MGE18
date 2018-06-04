productos = load 's3://esthefany-dpa/tarea4/input/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);
ordenes = load 's3://esthefany-dpa/tarea4/input/order_details.csv' using PigStorage(',') as (orderid:chararray,productid:chararray,unitprice:float,quantity:int, discount:float);

ordenes_agrupadas = group ordenes by productid;
cuenta_de_productos = FOREACH ordenes_agrupadas GENERATE group as productid,COUNT($1) as n;
ordenes_unidas = JOIN cuenta_de_productos by productid, productos by productid;
lista_ordenada = rank ordenes_unidas by n DESC;
rango_limitado = limit lista_ordenada 10;
resultado = FOREACH rango_limitado generate $0 as lugar, $1 as productid, $4 as nombre_de_producto, $2 as total_ordenes;
store resultado into 's3://esthefany-dpa/tarea4/output/ejercicioa' using PigStorage(',', '-schema');
