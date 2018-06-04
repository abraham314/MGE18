products = load '$INPUT/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);
order_details = load '$INPUT/order_details.csv' using PigStorage(',') as (orderid:chararray,productid:chararray,unitprice:float,quantity:int,	discount:float);
group_orders = group order_details by productid;
count_products = FOREACH group_orders GENERATE group as productid, COUNT($1) as n;
names_products = JOIN count_products by productid, products by productid;
ranked = rank names_products by n DESC;
limited_rank = limit ranked 10;
final = FOREACH limited_rank generate $0 as position, $4 as product, $2 as num_orders;

store final into '$OUTPUT/output2/' using PigStorage(',', '-schema');
