ELECT DISTINCT airlines.iata_code as codigo_iata, airlines.airline as aerolinea
FROM flights
JOIN airlines ON flights.airline = airlines.iata_code
JOIN airports ON flights.destination_airport = airports.iata_code
WHERE airports.airport in ("Honolulu International Airport");
