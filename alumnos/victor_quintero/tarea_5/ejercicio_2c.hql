create temporary table Honolulu as
SELECT day_of_week, airline, departure_delay
FROM flights
WHERE destination_airport IN (SELECT iata_code FROM airports WHERE airport in ("Honolulu International Airport"))
AND departure_delay > 0
AND flights.departure_delay IS NOT NULL;

SELECT honolulu.day_of_week as dia_semana, airlines.airline as aerolinea, round(avg(honolulu.departure_delay),2) as avg_delay
FROM honolulu
JOIN airlines ON honolulu.airline = airlines.iata_code
GROUP BY airlines.airline, honolulu.day_of_week
ORDER BY avg_delay ASC
LIMIT 1;
