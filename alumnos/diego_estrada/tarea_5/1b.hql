create table respuesta as select employeeid, count(*) as ordenes from orders 
group by employeeid order by ordenes desc limit 2;

create table datosordenes as select employeeid, ordenes, lag(ordenes, 1, 0) over() as nummaxordenes 
from respuesta order by ordenes asc limit 1;

SELECT A.employeeid as employeeid, A.firstname as nombre, A.lastname as apellido, A.title as titulo, A.hiredate as contratacion, B.nummaxordenes as primerlugarordenes, B.ordenes as ordenes 
FROM employees A  
JOIN datosordenes B 
ON (A.employeeid=B.employeeid);



