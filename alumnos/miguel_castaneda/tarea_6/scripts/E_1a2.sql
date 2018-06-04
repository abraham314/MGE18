%spark.sql

select e.employeeid, e.lastname, e.firstname, e.title, e.birthdate, e.hiredate, e.city, e.country, e.reportsto    
  from employees e 
  join (select distinct reportsto from employees) jefes 
  on jefes.reportsto = e.employeeid
