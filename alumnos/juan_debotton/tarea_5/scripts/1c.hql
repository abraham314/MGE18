CREATE TEMPORARY TABLE orders_delta AS
SELECT orderid,
from_utc_timestamp(date_format(orderdate,'yyyy-MM-dd HH:mm:ss.SSS'),'UTC') AS order_date,
datediff(orderdate, lag(orderdate) OVER (order by orderid)) AS delta
FROM orders
ORDER BY delta DESC;

SELECT MAX(delta) AS max_delta FROM orders_delta;

SELECT * FROM orders_delta WHERE delta IN (SELECT MAX(delta) AS delta FROM orders_delta);