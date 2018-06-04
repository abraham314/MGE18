SELECT t.id_jefe, t.nombre, t.apellido, t.f_nacimiento, f_inicio, t.ciudad, t.pais,nombre_subordinado FROM 
(
SELECT employeeid as id_jefe, a.firstname as nombre, a.lastname as apellido, a.title as titulo, a.birthdate as f_nacimiento, a.hiredate as f_inicio, a.city as ciudad, a.country as pais,b.subordinados as subordinado
FROM employees a 
JOIN
(SELECT reportsto, collect_set(lastname) as lastname, collect_set(firstname) AS subordinados
FROM employees
WHERE reportsto > 0
GROUP BY reportsto) b
ON a.employeeid = b.reportsto) t
LATERAL VIEW explode(t.subordinado) vis as nombre_subordinado
