use flights;

SELECT fly.day_of_week, air.airline, MAX(fly.departure_delay) mucho FROM airlines air JOIN flights fly ON (air.iata_code = fly.airline) GROUP BY fly.day_of_week, air.airline
ORDER BY mucho DESC
LIMIT 2;
