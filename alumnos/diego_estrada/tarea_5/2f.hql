CREATE TABLE vuelosaerolinea AS 
SELECT A.*, B.airline AS airlinename 
FROM flights A
LEFT OUTER JOIN airlines B ON (A.airline = B.iata_code);

create table retrasosprom as
select day_of_week, airlinename, avg(departure_delay) as retrasollegadaprom 
from vuelosaerolinea
group by airlinename, day_of_week;

create table resultado as 
select day_of_week, airlinename, retrasollegadaprom 
from retrasosprom
where day_of_week in ("2")
order by retrasollegadaprom asc
limit 3;

select * 
from resultado
order by retrasollegadaprom desc
limit 1;
