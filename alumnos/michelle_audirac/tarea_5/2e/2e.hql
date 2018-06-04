USE flights;

SELECT rank() OVER(PARTITION BY f.day_of_week ORDER BY f.d DESC) rank, f.day_of_week, f.airline, f.d
FROM (SELECT day_of_week, airline, avg(departure_delay) d
FROM flights
GROUP BY day_of_week, airline) f
ORDER BY rank
LIMIT 7;