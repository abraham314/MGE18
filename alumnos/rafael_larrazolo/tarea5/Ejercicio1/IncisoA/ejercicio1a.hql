SELECT employeeid, firstname as nombre, tabla1.lastname as apellido, tabla1.title as titulo, tabla1.birthdate as f_nacimiento, tabla1.hiredate as f_inicio, tabla1.city as ciudad, tabla1.country as pais
FROM employees tabla1 JOIN
(SELECT reportsto, collect_set(lastname) as lastname, collect_set(employeeid) AS subordinados
FROM employees
WHERE reportsto > 0
GROUP BY reportsto) tabla2
ON tabla1.employeeid = tabla2.reportsto
