SELECT count(f.destination_airport) n_incom_traffic, ap.airport
FROM flights f
JOIN airports ap
    ON f.destination_airport = ap.iata_code
GROUP BY f.destination_airport, ap.airport
ORDER BY n_incom_traffic DESC
LIMIT 1;
