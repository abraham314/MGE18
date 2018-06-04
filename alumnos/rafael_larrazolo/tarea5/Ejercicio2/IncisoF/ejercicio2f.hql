SELECT x.ranking as ranking, y.airline as airline, x.retraso_promedio as retraso_promedio FROM (
SELECT a.* FROM (
SELECT row_number() OVER (ORDER BY t.retraso_promedio) as ranking, t.airline, t.retraso_promedio FROM (
SELECT airline, round(avg(departure_delay),2) as retraso_promedio 
FROM vuelos
WHERE day_of_week = 2
GROUP BY airline
ORDER BY retraso_promedio ASC) t ) a
WHERE a.ranking = 3 ) x
JOIN
airlines y
ON
x.airline = y.iata_code
