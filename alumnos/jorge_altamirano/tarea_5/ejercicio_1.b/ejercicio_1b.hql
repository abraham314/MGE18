--- nombre, apellido, título, cuándo entró a la compañía, número de órdenes generadas, 
--- número de órdenes generadas por el mejor empleado (número 1)
SELECT 
    t2.firstname firstname, t2.lastname lastname, t2.title title,
    t2.hiredate hiredate, t1.n n_orders, t1.rank rank, 
    lead(t1.n) OVER (ORDER BY t1.n) maximum_sales_count
    FROM (SELECT  employeeid, 
        COUNT(employeeid) n, 
        rank() OVER (ORDER BY COUNT(o.employeeid) DESC) rank
        FROM orders o
        GROUP BY employeeid
        ORDER BY n DESC
        LIMIT 2) t1
    JOIN
        (SELECT employeeid, firstname, lastname, title, hiredate
        FROM employees) t2
    ON t1.employeeid = t2.employeeid
    GROUP BY t2.firstname, t2.lastname, t2.title,
    t2.hiredate, n, 
    rank
    --GROUP BY t1.employeeid, n, rank
    LIMIT 1;
