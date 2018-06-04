SELECT 
    CASE WHEN x.day_of_week = 1 THEN "Domingo" 
    WHEN x.day_of_week = 2 THEN "Lunes" 
    WHEN x.day_of_week = 3 THEN "Martes"
    WHEN x.day_of_week = 4 THEN "Miercoles"
    WHEN x.day_of_week = 5 THEN "Jueves"
    WHEN x.day_of_week = 6 THEN "Viernes"
    ELSE "Sabado" 
    END day_week,
    y.airline as airline,
    x.retraso_promedio as retraso_promedio 
FROM
    (SELECT a.airline, b.* 
    FROM(
        (SELECT airline, day_of_week, round(avg(departure_delay),2) as retraso_promedio 
        FROM vuelos
        GROUP BY airline, day_of_week)) a
    JOIN 
    (SELECT max(retraso_promedio) as retraso_promedio, day_of_week 
    FROM (
        SELECT airline, day_of_week, round(avg(departure_delay),2) as retraso_promedio 
    FROM vuelos
        GROUP BY airline, day_of_week) t
    GROUP BY day_of_week) b
    ON
    a.retraso_promedio = b.retraso_promedio) x
JOIN 
airlines y
ON
x.airline = y.iata_code
