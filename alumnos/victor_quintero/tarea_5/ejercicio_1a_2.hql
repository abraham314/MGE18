CREATE temporary table reporters as
SELECT reportsto, COLLECT_SET(lastname) as apellidos
from employees
WHERE reportsto > 0
GROUP BY reportsto; 

CREATE temporary table jointable as
SELECT employees.employeeid as num_empleado, 
        employees.firstname as nombre, 
        employees.lastname as apellido,  
        employees.title as titulo, 
        employees.birthdate as fecha_nacimiento, 
        employees.hiredate as fecha_ingreso, 
        employees.city as ciudad, 
        employees.country as pais,
	reporters.apellidos as subordinado
from employees
JOIN  reporters ON employees.employeeid = reporters.reportsto;

SELECT num_empleado,nombre,apellido,titulo,fecha_nacimiento,fecha_ingreso,ciudad,pais FROM jointable
LATERAL VIEW explode(subordinado) subview as sub;

