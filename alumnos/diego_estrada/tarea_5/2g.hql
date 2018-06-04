create table respuesta as
SELECT origin_airport, count(DISTINCT(destination_airport)) as destinos FROM flights
GROUP BY origin_airport
ORDER BY destinos DESC
LIMIT 1;

select a.destinos, b.airport as origen 
from respuesta A
LEFT OUTER JOIN
airports b
ON (a.origin_airport = b.iata_code)
LIMIT 1;
