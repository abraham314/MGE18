%spark.sql

select * from (
select  f.airline, a.airline, min(cast(f.departure_delay as int)) as menor
 from flights f 
 join airlines a 
 on a.iata_code = f.airline 
 where day_of_week = 2
 group by f.airline, a.airline
 order by menor asc limit 3) aerolineas 
 order by aerolineas.menor desc
 limit 1