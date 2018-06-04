SELECT avg(f.arrival_delay) delay, f.day_of_week, 
    f.airline, al.airline
FROM flights f
JOIN airlines al 
    ON f.airline = al.iata_code
WHERE destination_airport IN (
    SELECT iata_code
    FROM airports
    WHERE airport in ('Honolulu International Airport')
)
GROUP BY f.day_of_week, f.airline, al.airline
ORDER BY delay
LIMIT 1;
