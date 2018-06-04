CREATE temporary table orders_fecha as 
SELECT orderid, from_utc_timestamp(date_format(orderdate,'yyyy-MM-dd HH:mm:ss.SSS'),'UTC') as order_date, lag(from_utc_timestamp(date_format(orderdate,'yyyy-MM-dd HH:mm:ss.SSS'),'UTC')) OVER (partition by NULL) as order_date2 
from orders
ORDER BY order_date asc;

SELECT max(datediff(order_date, order_date2)) as delta 
FROM orders_fecha;
