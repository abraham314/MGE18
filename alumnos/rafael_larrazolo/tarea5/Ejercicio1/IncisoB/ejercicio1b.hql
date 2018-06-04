SELECT a.employeeid as employee_id ,b.firstname as nombre, b.lastname as apellido, b.title as titulo, b.hiredate as f_ingreso,  a.n as num_ordenes, a.maximo_ordenes as ordenes_mejor_empleado 
FROM
(SELECT result.* FROM
(SELECT p.employeeid, lead(p.n) OVER(ORDER BY p.n) maximo_ordenes, p.n FROM
(SELECT employeeid, count(employeeid) as n
FROM orders
GROUP BY employeeid
ORDER BY n DESC) p
ORDER BY p.n DESC
LIMIT 2) result
SORT BY n LIMIT 1) a
JOIN
(SELECT employeeid,firstname, lastname, title, hiredate
FROM employees) b
ON a.employeeid = b.employeeid

