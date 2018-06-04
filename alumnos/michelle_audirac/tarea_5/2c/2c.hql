USE flights;

SELECT avg(arrival_delay) d, day_of_week, airline
FROM flights
WHERE destination_airport = 'HNL'
GROUP BY day_of_week, airline
ORDER BY d
LIMIT 3;