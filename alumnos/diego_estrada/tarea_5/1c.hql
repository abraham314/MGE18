create table laggeada as SELECT orderid, orderdate, lag(orderdate) OVER(ORDER BY orderid) as laggazo FROM orders;

select orderid, orderdate, datediff(orderdate, laggazo) as tiempoalasigiente from laggeada order by tiempoalasigiente desc limit 1;

