USE flights;

SELECT origin_airport, count(DISTINCT destination_airport) n
FROM flights
GROUP BY origin_airport
ORDER BY n DESC
LIMIT 3;