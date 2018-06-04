SELECT f.day_of_week, f.airline, a.airline, AVG(f.departure_delay) AS avg_delay
FROM flights f
JOIN airlines a
ON a.iata_code = f.airline
WHERE f.destination_airport = 'HNL'
GROUP BY f.day_of_week, f.airline, a.airline
ORDER BY avg_delay ASC;