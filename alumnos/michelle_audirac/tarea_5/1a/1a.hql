USE northwind;

SELECT DISTINCT r.headid, e.firstname, e.lastname, e.title, e.birthdate, e.hiredate, e.city, e.country
FROM (SELECT explode(collect_set(reportsto)) as headid FROM employees) r JOIN employees e
ON (r.headid = e.employeeid);