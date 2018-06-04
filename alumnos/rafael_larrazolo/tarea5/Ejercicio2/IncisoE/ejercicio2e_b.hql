SELECT b.airline as airline ,
CASE WHEN x.day_of_week = 1 THEN "Domingo" 
    WHEN x.day_of_week = 2 THEN "Lunes" 
    WHEN x.day_of_week = 3 THEN "Martes"
    WHEN x.day_of_week = 4 THEN "Miercoles"
    WHEN x.day_of_week = 5 THEN "Jueves"
    WHEN x.day_of_week = 6 THEN "Viernes"
    ELSE "Sabado" 
    END day_week, 
x.maximo_dia as maximo_retraso_dia
FROM (
SELECT b.airline ,a.day_of_week, a.maximo_dia FROM
(SELECT day_of_week, max(departure_delay) as maximo_dia
FROM vuelos a
GROUP BY day_of_week) a
JOIN
vuelos b 
ON
a.day_of_week = b.day_of_week AND a.maximo_dia = b.departure_delay) x
JOIN
airlines b 
ON
x.airline = b.iata_code

