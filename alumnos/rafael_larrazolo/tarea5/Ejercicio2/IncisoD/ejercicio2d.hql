SELECT a.destination_airport as destination_airport, b.airport as airport, trafico_entrada FROM
(SELECT destination_airport, count(destination_airport) as trafico_entrada FROM vuelos
GROUP BY destination_airport
ORDER BY trafico_entrada DESC
LIMIT 1) a
JOIN
airports b ON
a.destination_airport = b.iata_code
