-- a) que aerolíneas llegan al aeropuerto de Honolulu BIEN
SELECT DISTINCT al.airline
FROM
flightss f JOIN airports ap
ON f.destination_airport = ap.iata_code
JOIN airlines al
ON f.airline=al.iata_code
WHERE ap.airport="Honolulu International Airport";

-- b ) ¿En qué horario (hora del día, no importan los minutos) hay salidas del aeropuerto de San Francisco (“SFO”) a “Honolulu International Airport”?
SELECT DISTINCT SUBSTR(f.scheduled_departure,0,2) as hour
FROM flightss f 
JOIN airports ap 
ON f.destination_airport = ap.iata_code
JOIN airports ap2
ON f.origin_airport = ap2.iata_code
JOIN airlines al
ON f.airline=al.iata_code
WHERE ap.airport="Honolulu International Airport" AND ap2.airport="San Francisco International Airport"
ORDER BY hour;

-- c) ¿Qué día de la semana y en qué aerolínea nos conviene viajar a “Honolulu International Airport” para tener el menor retraso posible?
SELECT al.airline, f.day_of_week, AVG(f.departure_delay) as fdelay
FROM
flightss f JOIN airports ap
ON f.destination_airport = ap.iata_code
JOIN airlines al
ON f.airline=al.iata_code
WHERE ap.airport="Honolulu International Airport"
GROUP BY al.airline, f.day_of_week
HAVING fdelay > 0
ORDER BY fdelay
LIMIT 10;

-- d) ¿Cuál es el aeropuerto con mayor tráfico de entrada?
SELECT ap.airport, COUNT(*) as traffic
FROM
flightss f JOIN airports ap
ON f.destination_airport = ap.iata_code
GROUP BY ap.airport
ORDER BY traffic DESC
LIMIT 10;

-- e)¿Cuál es la aerolínea con mayor retraso de salida por día de la semana?
CREATE TEMPORARY TABLE tmp AS
SELECT al.airline, f.day_of_week, AVG(f.departure_delay) as fdelay
FROM
flightss f JOIN airlines al
ON f.airline=al.iata_code
GROUP BY al.airline, f.day_of_week;

CREATE TEMPORARY TABLE tmp2 AS
SELECT day_of_week, MAX(fdelay) as mdelay
FROM tmp
GROUP BY day_of_week;

SELECT a.airline, a.day_of_week, b.mdelay
FROM tmp a 
JOIN tmp2 b 
ON a.fdelay = b.mdelay
ORDER BY a.day_of_week;



-- f) ¿Cuál es la tercer aerolínea con menor retraso de salida los lunes (day of week = 2)?
SELECT al.airline, f.day_of_week, AVG(f.departure_delay) as fdelay
FROM
flightss f 
JOIN airlines al
ON f.airline=al.iata_code
WHERE f.day_of_week = 2
GROUP BY al.airline, f.day_of_week
HAVING fdelay > 0
ORDER BY fdelay
LIMIT 3;

-- g) ¿Cuál es el aeropuerto origen que llega a la mayor cantidad de aeropuertos destino diferentes?
SELECT ap.airport, COUNT( DISTINCT f.destination_airport) as dests
FROM
flightss f JOIN airports ap
ON f.origin_airport = ap.iata_code
GROUP BY ap.airport
ORDER BY dests DESC
LIMIT 10;
