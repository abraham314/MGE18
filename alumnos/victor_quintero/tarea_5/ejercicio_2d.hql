SELECT airports.airport as aeropuerto, count(flights.destination_airport) as num_entradas
FROM flights
JOIN airports ON flights.destination_airport = airports.iata_code
GROUP BY flights.destination_airport, airports.airport
ORDER BY num_entradas DESC 
LIMIT 1;
