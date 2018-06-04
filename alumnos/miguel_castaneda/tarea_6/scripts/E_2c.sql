%spark.sql

select f.day_of_week, f.airline, a.airline, min(cast (f.departure_delay as int))  as minimo
 from flights f 
 join airlines a 
 on a.iata_code = f.airline 
where f.destination_airport ='HNL' 
group by f.day_of_week, f.airline, a.airline
order by minimo asc limit 1