use flights; 

select  f.airline, a.airline, min(f.departure_delay) as menor
 from flights f 
 join airlines a 
 on a.iata_code = f.airline 
 where day_of_week = 2
 group by f.airline, a.airline
 order by menor asc limit 1 offset 2;