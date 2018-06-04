CREATE TABLE vuelosaerolineas AS SELECT A.destination_airport as iatadestino, B.airline as aerolinea
FROM flights A
LEFT OUTER JOIN airlines B ON (A.airline = B.iata_code);

select distinct A.aerolinea as aerolinea FROM vuelosaerolineas A 
LEFT OUTER JOIN airports B 
on (A.iatadestino = B.iata_code) 
WHERE B.airport IN ("Honolulu International Airport");
