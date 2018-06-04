SELECT DISTINCT(floor(f.scheduled_departure / 100)) AS departures
FROM flights f
WHERE f.destination_airport IN (
    SELECT iata_code
    FROM airports
    WHERE airport in ('Honolulu International Airport')
) AND 
    f.origin_airport IN ('SFO');
