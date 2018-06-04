USE northwind;

SELECT orderid, orderdate, lag(orderdate) OVER(ORDER BY orderid), 
datediff(orderdate, lag(orderdate) OVER(ORDER BY orderid)) delta
FROM orders 
ORDER BY delta DESC
LIMIT 5;