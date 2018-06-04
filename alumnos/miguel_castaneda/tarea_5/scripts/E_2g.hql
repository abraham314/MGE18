use flights; 

select a.airport, count(distinct f.destination_airport) as total_destinos
 from flights f 
 join airports a 
 on a.iata_code = f.origin_airport 
 group by a.airport
 order by total_destinos desc limit 1;