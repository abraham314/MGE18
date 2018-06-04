%spark.sql

select a.employeeid, concat(e.lastname ," ",  e.firstname) as nombre, e.title, e.hiredate, a.total, lead(a.total,1) over (order by a.total) as primero 
from ( 
  select o.employeeid, count(*) as total 
  from orders o 
  group by o.employeeid
) a 
join employees e
on e.employeeid = a.employeeid
order by primero desc limit 1   