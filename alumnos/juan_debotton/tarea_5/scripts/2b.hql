SELECT DISTINCT SUBSTRING(LPAD(scheduled_departure,4,"0"),1,2) AS hour
FROM flights f WHERE f.destination_airport='HNL' and f.origin_airport='SFO';