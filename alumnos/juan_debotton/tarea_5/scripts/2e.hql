SELECT f.airline, a.airline, f.day_of_week, f.departure_delay FROM flights f
JOIN (
	SELECT day_of_week, MAX(departure_delay) AS maximo FROM flights 
	GROUP BY day_of_week
) ff
ON f.departure_delay = ff.maximo
JOIN airlines a
ON f.airline = a.iata_code
ORDER BY f.day_of_week;