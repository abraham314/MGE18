use flights;

select  f.destination_airport, a.airport, count(f.destination_airport) as total_llegadas
 from flights f 
 join airports a 
 on a.iata_code = f.destination_airport 
 group by f.destination_airport, a.airport
 order by total_llegadas desc limit 1;