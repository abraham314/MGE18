SELECT f.airline, a.airline, min(f.departure_delay) AS menor
FROM flights f
JOIN airlines a
ON a.iata_code = f.airline
WHERE day_of_week = 2
GROUP BY f.airline, a.airline
ORDER BY menor ASC
LIMIT 3;

