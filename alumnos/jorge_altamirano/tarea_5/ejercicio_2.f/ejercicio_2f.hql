SELECT f.iata_code iata_code, al.airline airline
FROM (
SELECT 
    rank() OVER (ORDER BY avg(departure_delay) DESC) rank,
    airline iata_code
FROM flights
WHERE day_of_week = 2 
GROUP BY airline, airline
LIMIT 3
) f 
JOIN airlines al 
    ON f.iata_code = al.iata_code
WHERE f.rank = 3
GROUP BY f.iata_code, al.airline;
