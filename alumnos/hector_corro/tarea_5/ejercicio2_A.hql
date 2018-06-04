use flights;

SELECT DISTINCT(vuelo.airline) as airline, a.airline FROM flights vuelo 
JOIN airlines a 
on a.iata_code = vuelo.airline
WHERE destination_airport = 'HNL'; 
