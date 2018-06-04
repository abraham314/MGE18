SELECT b.airport as aeropuerto, a.distinct_destination as n_aeropuertos_destino FROM (
SELECT origin_airport, count(DISTINCT(destination_airport)) as distinct_destination FROM vuelos
GROUP BY origin_airport
ORDER BY distinct_destination DESC
LIMIT 1) a
JOIN
airports b
ON
a.origin_airport = b.iata_code
