SELECT t2.orderid orderid, 
    datediff(from_utc_timestamp(date_format("2018-03-03",'yyyy-MM-dd HH:mm:ss.SSS'),'UTC'), orderdate) delta
FROM (
    SELECT t1.orderid,
        from_utc_timestamp(date_format(orderdate,'yyyy-MM-dd HH:mm:ss.SSS'),'UTC') orderdate,
        from_utc_timestamp(date_format(lag(orderdate) OVER(ORDER BY t1.orderid),'yyyy-MM-dd HH:mm:ss.SSS'),'UTC') delta
        FROM orders t1
    ) t2
ORDER BY delta DESC
LIMIT 1;
