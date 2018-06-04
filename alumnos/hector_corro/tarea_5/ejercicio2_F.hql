use flights; 

SELECT  fly.airline, air.airline, MIN(fly.departure_delay) AS optimo FROM flights fly JOIN airlines air ON air.iata_code = fly.airline WHERE day_of_week = 2
GROUP BY fly.airline, air.airline
ORDER BY optimo ASC
LIMIT 1 OFFSET 2;
