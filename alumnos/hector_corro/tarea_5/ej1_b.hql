use northwind;

select a.employeeid, concat(e.lastname ," ",  e.firstname) as name, e.title, e.hiredate, a.total, lead(a.total,1) over (order by a.total) as first
    from ( 
    select o.employeeid, count(*) as total
    from orders o
    group by o.employeeid
    ) a
    join employees e
    on e.employeeid = a.employeeid
    order by first desc limit 1;

