SELECT DISTINCT CASE
    WHEN LENGTH(cast(flights.departure_time as string)) = "3" then cast(substr(flights.departure_time, 1, 1) as int) 
    WHEN LENGTH(cast(flights.departure_time as string)) = "4" then cast(substr(flights.departure_time, 1, 2) as int)
    ELSE 0
    END as horario
FROM flights
WHERE destination_airport in(SELECT iata_code FROM airports WHERE airport in ("Honolulu International Airport")) 
AND origin_airport in("SFO")
AND flights.departure_time IS NOT NULL
ORDER BY horario ASC;
