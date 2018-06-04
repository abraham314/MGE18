SELECT f.origin_airport, ap.airport, count(f.destination_airport) AS n_destination
FROM flights f
JOIN airports ap
    ON f.origin_airport = ap.iata_code
GROUP BY f.origin_airport, ap.airport
ORDER BY n_destination DESC
LIMIT 1;
