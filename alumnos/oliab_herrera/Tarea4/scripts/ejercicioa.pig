--borrar la salida
rmf $OUTPUT

order_details = load '$INPUT/order_details' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);

products = load '$INPUT/products' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);


order_grp = group order_details by productid;
count_products = FOREACH order_grp GENERATE group as productid,COUNT($1) as n;



ranked = rank count_products by n DESC;
top = LIMIT ranked 5;

join_products= JOIN top by productid, products by productid;

rank_final= rank join_products by n DESC;


store join_products into '$OUTPUT' using PigStorage(',', '-schema');