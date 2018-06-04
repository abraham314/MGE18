-- a. Número de jefes
-- Regresa el número de jefes
SELECT COUNT(DISTINCT reportsto)
FROM employees
WHERE reportsto <> '';


-- Cuales son estos jefes
CREATE TEMPORARY TABLE tmp1a AS
SELECT * FROM (SELECT employeeid, firstname, lastname, title, birthdate, hiredate, city, country
FROM employees) a
JOIN
(SELECT reportsto, collect_list(firstname) as subs
FROM employees
WHERE reportsto <> ''
GROUP BY reportsto) b 
ON a.employeeid=b.reportsto;

SELECT employeeid, firstname, lastname, title, birthdate, hiredate, city, country, sub
FROM tmp1a
LATERAL VIEW explode(subs) subView AS sub;

-- b. Quien es el segundo mejor empleado que más órdenes a generado
CREATE TEMPORARY TABLE tmp1b AS
SELECT b.firstname, b.lastname, b.title, b.hiredate, a.sales, LEAD(sales) OVER(ORDER BY sales) max
FROM (SELECT employeeid, COUNT(*) as sales
FROM orders
GROUP BY employeeid) a
JOIN
(SELECT employeeid as e2, firstname, lastname, title, hiredate
FROM employees) b
ON a.employeeid=b.e2
GROUP BY b.firstname, b.lastname, b.title, b.hiredate, a.sales
ORDER BY sales DESC;

SELECT * FROM 
(SELECT row_number() OVER (partition by 1) rn, d.* FROM tmp1b d) q 
WHERE q.rn = 2;

-- c) ¿Cuál es el delta de tiempo más grande entre una orden y otra?
CREATE TEMPORARY TABLE tmp1c AS
SELECT orderid, cast(to_date(from_unixtime(unix_timestamp(orderdate, 'yyyy-MM-dd'))) as date) as dateorder
FROM orders;

SELECT t.orderid, datediff(t.dateorder, t.dif) as delta
FROM(
SELECT orderid, dateorder, (LAG(dateorder) OVER(ORDER BY orderid)) as dif
FROM tmp1c) t
ORDER BY delta DESC
LIMIT 10;

