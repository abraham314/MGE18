CREATE temporary table retrasos as
SELECT airlines.airline as aerolinea, round(avg(flights.departure_delay),2) as avg_delay
FROM flights
JOIN airlines ON flights.airline = airlines.iata_code
WHERE departure_delay > 0
AND day_of_week = 2
AND flights.departure_delay IS NOT NULL
GROUP BY airlines.airline
ORDER BY avg_delay ASC;

CREATE temporary table retrasosranking as
SELECT row_number() OVER (ORDER BY retrasos.avg_delay) as ranking, aerolinea, avg_delay 
FROM retrasos;

SELECT * FROM retrasosranking
WHERE ranking = 3;
