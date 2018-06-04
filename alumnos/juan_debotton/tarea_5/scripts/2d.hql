SELECT f.destination_airport, a.airport, COUNT(f.destination_airport) AS total_llegadas
FROM flights f
JOIN airports a
ON a.iata_code = f.destination_airport
GROUP BY f.destination_airport, a.airport
ORDER BY total_llegadas DESC LIMIT 10;