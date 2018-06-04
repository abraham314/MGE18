USE flights;

SELECT a.airline 
FROM (SELECT DISTINCT airline 
FROM flights 
WHERE destination_airport = 'HNL') h LEFT JOIN airlines a
ON (h.airline = a.iata_code);