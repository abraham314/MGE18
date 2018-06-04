use flights;

SELECT fly.destination_airport, air.airport, count(fly.destination_airport) as arrival FROM flights fly
JOIN airports air 
ON air.iata_code = fly.destination_airport
GROUP BY fly.destination_airport, air.airport ORDER BY arrival DESC LIMIT 1;


