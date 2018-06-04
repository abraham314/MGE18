use flights; 

SELECT air.airport, COUNT(DISTINCT fly.destination_airport) AS destinos FROM flights fly JOIN airports air ON air.iata_code = fly.origin_airport 
GROUP BY air.airport
ORDER BY destinos DESC
limit 1; 
