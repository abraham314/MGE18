USE flights;

SELECT DISTINCT(f.airline) as hnl_airlines, a.airline AS hnl_airline_name FROM flights f
JOIN airlines a
ON a.iata_code = f.airline
WHERE destination_airport = 'HNL';