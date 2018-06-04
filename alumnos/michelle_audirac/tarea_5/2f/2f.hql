USE flights;

SELECT rank() OVER (ORDER BY avg(departure_delay)), airline
FROM flights
WHERE day_of_week = 2 
GROUP BY airline
LIMIT 5;