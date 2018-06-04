use flights; 

select f.airline, a.airline, f.day_of_week , f.departure_delay from flights f 
    join (
    select day_of_week, max(departure_delay) as maximo from flights group by day_of_week
    ) ff 
    on  f.departure_delay = ff.maximo
    join airlines a 
    on f.airline = a.iata_code
    order by f.day_of_week;