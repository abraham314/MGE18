%spark.sql

select distinct(f.airline) as airline, a.airline from flights f 
join airlines a 
on a.iata_code = f.airline
where destination_airport = 'HNL'