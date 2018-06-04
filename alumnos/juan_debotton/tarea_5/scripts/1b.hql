SELECT b.employeeid, a.num_orders, b.firstname, b.lastname, b.title, b.hiredate FROM employees as b 
INNER JOIN (SELECT employeeid,COUNT(employeeid) AS num_orders FROM orders GROUP BY employeeid ORDER BY num_orders DESC LIMIT 2) as a 
ON a.employeeid = b.employeeid;