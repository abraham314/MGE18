%spark.sql

select f.airline, a.airline, f.day_of_week , cast(f.departure_delay as int) as departure_delay from flights f 
    join (
    select day_of_week, max(cast(departure_delay as int) ) as maximo from flights group by day_of_week
    ) ff 
    on  f.departure_delay = ff.maximo
    join airlines a 
    on f.airline = a.iata_code
    order by f.day_of_week