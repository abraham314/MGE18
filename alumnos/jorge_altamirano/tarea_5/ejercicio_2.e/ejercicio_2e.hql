SELECT avg(f.departure_delay) delay, f.day_of_week, 
    f.airline, al.airline
FROM flights f
JOIN airlines al 
    ON f.airline = al.iata_code
GROUP BY f.departure_delay, f.day_of_week, f.airline, al.airline
ORDER BY delay DESC
LIMIT 1;

SELECT DISTINCT f.day_of_week day_of_week, f.iata_code iata_code, al.airline airline
FROM (
    SELECT rank() OVER(PARTITION BY day_of_week ORDER BY avg(departure_delay) DESC) rank, 
        avg(departure_delay) delay,
        day_of_week day_of_week, 
        airline iata_code
    FROM flights
    GROUP BY day_of_week, airline
    ORDER BY rank, day_of_week, iata_code
    LIMIT 7
) f JOIN airlines al
ON f.iata_code = al.iata_code
WHERE f.rank = 1 
ORDER BY f.day_of_week;
