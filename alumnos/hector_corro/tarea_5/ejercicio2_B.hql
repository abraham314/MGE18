use flights;

SELECT DISTINCT substring(lpad(scheduled_departure,4,"0"),1,2) AS timevuelo 
FROM flights vuelo WHERE vuelo.destination_airport ='HNL' AND vuelo.origin_airport ='SFO'; 
