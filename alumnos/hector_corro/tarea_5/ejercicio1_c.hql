use northwind;

SELECT rango.orderid as orderid, datediff(rango.orderdate, rango.diference) as rango
FROM(SELECT orderid, orderdate, (LAG(orderdate) OVER(ORDER BY orderid)) as diference FROM orders) rango
ORDER BY rango DESC
LIMIT 1;
