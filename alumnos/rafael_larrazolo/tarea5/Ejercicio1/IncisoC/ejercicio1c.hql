SELECT t.order_id as order_id, datediff(t.order_date, t.delta) as delta FROM 
(SELECT orderid as order_id, order_date as order_date, lag(order_date) OVER(ORDER BY orderid) as delta FROM
orders2 ) t
ORDER BY delta DESC
LIMIT 10
