%spark.sql

select orderid, from_utc_timestamp(date_format(orderdate,'yyyy-MM-dd HH:mm:ss.SSS'),'UTC') as orderdate , 
datediff(orderdate, lag(orderdate) over (order by orderid)) as diferencia
from orders 
order by diferencia desc
limit 1