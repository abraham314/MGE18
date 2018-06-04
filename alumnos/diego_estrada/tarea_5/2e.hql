CREATE TABLE vuelosaerolinea AS 
SELECT A.*, B.airline AS airlinename 
FROM flights A
LEFT OUTER JOIN airlines B ON (A.airline = B.iata_code);

create table retrasosprom as
select day_of_week, airlinename, avg(departure_delay) as retrasollegadaprom 
from vuelosaerolinea
group by airlinename, day_of_week
ORDER BY retrasollegadaprom desc;

create table resultado as 
select day_of_week, airlinename, max(retrasollegadaprom) as retraso 
from retrasosprom
group by day_of_week, airlinename
order by retraso desc;

SELECT a.day_of_week, a.retrasomax, b.airlinename FROM
(SELECT day_of_week, MAX(retraso) as retrasomax FROM resultado
GROUP BY day_of_week) a
LEFT OUTER JOIN
retrasosprom b ON
a.retrasomax = b.retrasollegadaprom
