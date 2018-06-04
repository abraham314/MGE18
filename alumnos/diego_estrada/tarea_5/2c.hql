create table vuelosretraso as select day_of_week, airline, destination_airport, (departure_delay + arrival_delay) as retrasototal from flights;

CREATE TABLE retrasoaeropuerto AS SELECT A.destination_airport as iatadestino, A.airline, A.day_of_week, A.retrasototal, B.airport as airport
FROM vuelosretraso A
LEFT OUTER JOIN airports B ON (A.destination_airport = B.iata_code);

CREATE TABLE retrasopyl AS SELECT A.*, B.airline as airlinename 
FROM retrasoaeropuerto A
LEFT OUTER JOIN airlines B ON (A.airline = B.iata_code);

select avg(retrasototal) as retrasopromedio, day_of_week, airlinename 
from retrasopyl
where airport in ("Honolulu International Airport")
group by day_of_week, airlinename
order by retrasopromedio asc
limit 1;
