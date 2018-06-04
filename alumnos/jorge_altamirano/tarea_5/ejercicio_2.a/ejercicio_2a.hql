SELECT f.airline, al.airline
FROM flights f
JOIN airlines al 
    ON f.airline = al.iata_code
WHERE destination_airport IN (
    SELECT iata_code
    FROM airports
    WHERE airport in ('Honolulu International Airport')
)
GROUP BY f.airline, al.airline;
