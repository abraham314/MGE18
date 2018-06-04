SELECT a.airport, COUNT(DISTINCT f.destination_airport) AS num_destinos
FROM flights f
JOIN airports a
ON a.iata_code = f.origin_airport
GROUP BY a.airport
ORDER BY num_destinos DESC 
LIMIT 1;