--1.a.1) ¿Cuántos “jefes” hay en la tabla empleados?

select count(*) as num_jefes_distintos
from (
select distinct a.reportsto 
from employees a 
where a.reportsto IS NOT NULL) b;


--1.a.2) ¿Cuáles son estos jefes: 
--número de empleado, nombre, apellido, título, fecha de nacimiento, 
--fecha en que iniciaron en la empresa, ciudad y país? 
--(atributo reportsto, ocupa explode en tu respuesta)



create table northwind.aux_explode as 
select *
from 
(select employeeid, firstname, lastname, title, birthdate, hiredate, city, count(*) over () n_freq 
from employees where employeeid
in (select distinct reportsto from employees)) a
left join (select reportsto, collect_set(firstname) as subordinates from employees group by reportsto) b 
on  a.employeeid = b.reportsto;


select * from northwind.aux_explode 
LATERAL VIEW explode(subordinates) subordinates_a as subordinates_b;



--1.b) ¿Quién es el segundo “mejor” empleado que más órdenes ha generado? 
--(nombre, apellido, título, cuándo entró a la compañía, número de 
--órdenes generadas, número de órdenes generadas por el mejor empleado (número 1))
--
--+ Con el siguiente código obtenemos los 2 mejores empleados y su infomación: 
--el máximo es 156 y el segundo mejor empleado tuvo 127


select a.lastname, a.firstname, a.title, a.hiredate, c.*
from employees as a
inner join 
	(select z.employeeid, count(*) as cnt_orders
	from orders z
	group by z.employeeid
	order by cnt_orders desc
	limit 2) c
on a.employeeid = c.employeeid;   


--1.c) ¿Cuál es el delta de tiempo más grande entre una orden y otra?


select a.orderid, a.orderdate, datediff(a.orderdate, a.date2) as delta 
from 
(select z.orderid, z.orderdate, lag(z.orderdate) OVER(ORDER BY z.orderid) as date2 FROM orders z) a
order by delta desc limit 1;
