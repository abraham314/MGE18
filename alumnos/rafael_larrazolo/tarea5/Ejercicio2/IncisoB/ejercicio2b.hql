SELECT DISTINCT(cast(substr(departure_time, 1, 2) as int)) as hora_dia, origin_airport, destination_airport FROM vuelos 
WHERE destination_airport in(
SELECT iata_code FROM airports
WHERE city in ("Honolulu")) 
AND origin_airport in(
SELECT iata_code FROM airports
WHERE city in ("San Francisco"))
ORDER BY hora_dia 
