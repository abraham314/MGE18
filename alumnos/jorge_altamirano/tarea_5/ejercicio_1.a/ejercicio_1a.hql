--- Cuenta de empleados
SELECT COUNT(DISTINCT(t1.reportsto))
    FROM employees t1
    WHERE char_length(reportsto) > 0;
    
--- Datos solicitados: (sin explode)
--- número de empleado, nombre, apellido, título, 
--- fecha de nacimiento, aniv empresa, ciudad y país
SELECT t2.reportsto employeeid, t1.firstname, t1.lastname,
    t1.birthdate, t1.hiredate, t1.city, t1.country
    FROM employees t1
    JOIN (
        SELECT reportsto
        FROM employees
        WHERE char_length(reportsto) > 0
    ) t2
    ON t2.reportsto = t1.employeeid
    GROUP BY t2.reportsto, t1.firstname, t1.lastname,
    t1.birthdate, t1.hiredate, t1.city, t1.country;
    
--- Datos solicitados: (con explode)
--- número de empleado, nombre, apellido, título, 
--- fecha de nacimiento, aniv empresa, ciudad y país
--- SELECT collect_set(reportsto) 
---    FROM employees 
---    WHERE char_length(reportsto) > 0;
SELECT t.employeeid employeeid, t.firstname firstname, 
    t.lastname lastname, t.title title, t.birthdate birthdate,
    t.hiredate hiredate, t.city city, t.country country,
    v1.reportees reportees FROM
(
  SELECT t1.employeeid, t1.firstname, t1.lastname, 
    t1.title as title, t1.birthdate, t1.hiredate,
    t1.city, t1.country, t2.reportee
  FROM employees t1
  JOIN
  (
    SELECT reportsto, collect_list((firstname||' ')||lastname) AS reportee
    FROM employees
    WHERE reportsto > 0
    GROUP BY reportsto
  ) t2
  ON t1.employeeid = t2.reportsto
) t
LATERAL VIEW explode(t.reportee) v1 as reportees;
