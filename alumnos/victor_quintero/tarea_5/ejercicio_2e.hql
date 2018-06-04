CREATE temporary table retrasos as
SELECT day_of_week, airline, departure_delay 
FROM flights
WHERE departure_delay > 0
AND flights.departure_delay IS NOT NULL;

CREATE temporary table retrasosgroup as
SELECT retrasos.day_of_week as dia_semana, airlines.airline as aerolinea, round(avg(retrasos.departure_delay),2) as avg_delay
FROM retrasos
JOIN airlines ON retrasos.airline = airlines.iata_code
GROUP BY airlines.airline, retrasos.day_of_week
ORDER BY avg_delay ASC;

CREATE temporary table retrasomax as
SELECT dia_semana, aerolinea, max(avg_delay) as max_delay
FROM retrasosgroup
group by dia_semana, aerolinea;

CREATE temporary table maxdia as
SELECT dia_semana, MAX(max_delay) as max_retraso
FROM retrasomax
GROUP BY dia_semana;

SELECT maxdia.dia_semana as dia_semana, retrasomax.aerolinea as aerolinea, maxdia.max_retraso as mayor_retraso
FROM maxdia
JOIN retrasomax ON  maxdia.max_retraso = retrasomax.max_delay
ORDER BY dia_semana asc;
