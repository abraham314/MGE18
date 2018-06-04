CREATE temporary table numordenes as
SELECT employeeid, count(employeeid) as n
FROM orders
GROUP BY employeeid
ORDER BY n DESC
LIMIT 2;

CREATE temporary table toporders as
SELECT numordenes.employeeid, lead(numordenes.n) OVER(ORDER BY numordenes.n) as ordenes_primero, numordenes.n 
FROM numordenes
ORDER BY numordenes.n ASC
LIMIT 1;

SELECT employees.firstname as nombre,
        employees.lastname as apellido, 
        employees.title as titulo,
        employees.hiredate as fecha_ingreso,
        toporders.n as num_ordenes,
        toporders.ordenes_primero as num_ordenes_mejor_empleado
from toporders
JOIN employees ON toporders.employeeid = employees.employeeid;
