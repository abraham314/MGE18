USE flights;

SELECT DISTINCT floor(scheduled_departure / 100)
FROM flights 
WHERE destination_airport = 'HNL'
AND origin_airport = 'SFO';