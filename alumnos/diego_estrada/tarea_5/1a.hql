
create table respuesta as 
select *
from 
    (select employeeid, firstname, lastname, title, birthdate, hiredate, city, count(*) over () totalJefes 
from employees where employeeid
in (select distinct reportsto from employees)) t1
left join (select reportsto, collect_set(firstname) as subordinados from employees group by reportsto) t2 
on  t1.employeeid = t2.reportsto;


SELECT * FROM respuesta 
LATERAL VIEW explode(subordinados) subordinadosarray as subordinadoselement;
