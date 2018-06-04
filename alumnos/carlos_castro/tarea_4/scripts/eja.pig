rmf $OUTPUT

order_details = load '$INPUT/order_details.csv' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);
order_details_X = FOREACH order_details GENERATE productid;

products = load '$INPUT/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);
products_X = FOREACH products GENERATE productid, productname;


order_grp = group order_details_X by productid;
count_products = FOREACH order_grp GENERATE group as productid,COUNT($1) as n;



ranked = rank count_products by n DESC;
top = LIMIT ranked 100;

join_products= JOIN top by productid, products_X by productid;

rank_final= rank join_products by n DESC;


store rank_final into '$OUTPUT';