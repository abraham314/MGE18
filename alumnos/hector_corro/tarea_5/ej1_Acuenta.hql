use northwind;

select count(*) as boss_count
  from employees emp 
  join (select distinct reportsto from employees) bosses 
  on bosses.reportsto = emp.employeeid; 
