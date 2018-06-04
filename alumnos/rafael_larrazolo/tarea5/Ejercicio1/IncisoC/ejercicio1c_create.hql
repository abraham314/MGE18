create table orders2 as select orderid, from_utc_timestamp(date_format(orderdate,'yyyy-MM-dd HH:mm:ss.SSS'),'UTC') as order_date from orders;
