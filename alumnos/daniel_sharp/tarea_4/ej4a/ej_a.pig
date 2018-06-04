-- Daniel Sharp 138176
-- Se cargan ambas tablas a Pig
order_details = load 's3://daniel-sharp/t4ej_a/inputs/order_details.csv' using PigStorage(',') as (orderid:chararray, productid:chararray, unitprice:float, quantity:int, discount:float);

products = load 's3://daniel-sharp/t4ej_a/inputs/products.csv' using PigStorage(',') as (productid:chararray, productname:chararray, supplierid:chararray, categoryid:chararray, quantityperunit:int, unitprice:float, unitsinstock:int, unitsonorder:int, reorderlevel:int, discounted:int);

-- Se hace el join para poder crear la lista rankeada por el nombre
order_details_pname = join order_details by productid, products by productid; 

-- Se agrupan por nombre de product
group_orders = group order_details_pname by productname;

-- Se lleva a cabo la cuenta de ocurrencias de cada producto
count_products_res = FOREACH group_orders GENERATE group as productname, COUNT($1) as c;

-- Se ordenan de mayor a menor 
ranked = rank count_products_res by c DESC;

-- Se seleccionan las 10 primeras
limited_ranked = LIMIT ranked 10;

-- Se guarda el archivo resultante
store limited_ranked into 's3://daniel-sharp/t4ej_a/output/';


