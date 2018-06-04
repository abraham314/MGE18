CREATE table destinovuelos as 
SELECT destination_airport, count(destination_airport) as trafico FROM flights
GROUP BY destination_airport
ORDER BY trafico DESC
LIMIT 1;

select B.airport FROM destinovuelos A 
LEFT OUTER JOIN airports B 
on (A.destination_airport = B.iata_code); 
