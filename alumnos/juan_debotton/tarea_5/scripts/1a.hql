USE northwind;

SELECT DISTINCT e.employeeid, e.firstname, e.lastname, e.title, e.birthdate, e.hiredate, e.city, e.country FROM northwind.employees e
INNER JOIN (SELECT DISTINCT a.employeeid, a.report as boss FROM 
(SELECT employeeid, report FROM northwind.employees e LATERAL VIEW explode(e.reportsto) adTable AS report) as a 
WHERE a.report IS NOT NULL) as b 
ON e.employeeid = b.boss;