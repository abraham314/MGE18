CREATE temporary table destinos as
SELECT origin_airport, count(DISTINCT(destination_airport)) as n_destinos 
FROM flights
GROUP BY origin_airport
ORDER BY n_destinos DESC
LIMIT 1;

SELECT airports.airport as aeropuerto, destinos.n_destinos as destinos_diferentes
FROM destinos
JOIN airports ON destinos.origin_airport = airports.iata_code;
