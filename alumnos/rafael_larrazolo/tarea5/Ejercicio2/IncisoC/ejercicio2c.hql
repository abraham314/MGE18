SELECT b.airline, day_week  FROM(
SELECT t.airline as airline, 
    CASE WHEN t.day_of_week = 1 THEN "Domingo" 
    WHEN t.day_of_week = 2 THEN "Lunes" 
    WHEN t.day_of_week = 3 THEN "Martes"
    WHEN t.day_of_week = 4 THEN "Miercoles"
    WHEN t.day_of_week = 5 THEN "Jueves"
    WHEN t.day_of_week = 6 THEN "Viernes"
    ELSE "Sabado" 
    END day_week, 
    round(avg(t.departure_delay),2) as retraso_prom FROM (
SELECT day_of_week, airline, departure_delay FROM vuelos
WHERE destination_airport IN (
SELECT iata_code FROM airports
WHERE airport in ("Honolulu International Airport"))) t
GROUP BY t.airline, t.day_of_week
SORT BY retraso_prom
LIMIT 1) a
JOIN
airlines b ON
a.airline = b.iata_code
