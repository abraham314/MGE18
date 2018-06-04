USE northwind;

SELECT DISTINCT e.firstname, e.lastname, e.title, e.hiredate, q.ce1, q.cn1, q.cn0 
FROM (SELECT c.e[1] as ce1, c.n[1] as cn1, c.n[0] as cn0 
FROM (SELECT collect_set(o.employeeid) as e, collect_set(o.n) as n 
FROM (SELECT employeeid, COUNT(employeeid) n 
FROM orders
GROUP BY employeeid
ORDER BY n DESC) o) c) q JOIN employees e
ON (q.ce1==e.employeeid);