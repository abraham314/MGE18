%spark.sql

select count(*) as total_jefes
  from employees e 
  join (select distinct reportsto from employees) jefes 
  on jefes.reportsto = e.employeeid