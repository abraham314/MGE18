products = load 's3://tarea4/bases/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);
order_details = load 's3://tarea4/bases/order_details.csv' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);
order_grp = group order_details by productid;
a = FOREACH order_grp GENERATE group as productid,COUNT($1) as total; 
ranking =  rank a by total DESC;
join_products = JOIN ranking by productid, products by productid;
por_nombre = FOREACH join_products generate productname,total; 
rank_name = rank por_nombre by total DESC;
winner = limit rank_name 1; 
STORE winner INTO 's3://tarea4/outputs/ejer_a' USING PigStorage(',');



 
