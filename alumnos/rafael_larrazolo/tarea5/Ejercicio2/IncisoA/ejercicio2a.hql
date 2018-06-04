SELECT a.airline as iata_code, b.airline as airline FROM 
(SELECT DISTINCT(airline) FROM vuelos WHERE destination_airport in(
(SELECT iata_code FROM airports
WHERE airport in ("Honolulu International Airport")))) a
JOIN
airlines b ON
a.airline = b.iata_code

