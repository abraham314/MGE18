USE flights;

SELECT destination_airport , count(destination_airport) n
FROM flights 
GROUP BY destination_airport
ORDER BY n DESC
LIMIT 3;