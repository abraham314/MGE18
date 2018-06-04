CREATE TABLE vuelosaeropuertosD AS 
SELECT A.origin_airport, A.destination_airport, A.scheduled_departure, B.airport AS nombredestino
FROM flights A
LEFT OUTER JOIN airports B ON (A.destination_airport = B.iata_code);

CREATE TABLE vuelosaeropuertosDO AS 
SELECT A.*, B.airport AS nombreorigen
FROM vuelosaeropuertosD A
LEFT OUTER JOIN airports B ON (A.origin_airport = B.iata_code);

select distinct (substr(scheduled_departure, 1, 2)) as horassalida from vuelosaeropuertosDO 
WHERE nombredestino IN ("Honolulu International Airport") 
AND nombreorigen IN ("San Francisco International Airport");
