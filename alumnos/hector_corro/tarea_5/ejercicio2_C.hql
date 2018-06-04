use flights;

SELECT flight.day_of_week, flight.airline, air.airline, MIN(flight.departure_delay) as retraso
 from flights flight join airlines air on air.iata_code = flight.airline 
WHERE flight.destination_airport ='HNL' 
group by flight.day_of_week, flight.airline, air.airline
order by retraso asc limit 1; 
