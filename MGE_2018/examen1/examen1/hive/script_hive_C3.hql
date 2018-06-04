select year(a.fech_d) as anio,count(*) as fases from(select id_parameter,
to_date(from_unixtime(unix_timestamp(`date`,'dd/MM/yyyy HH:mm'))) as fech_d,max(value*0.001*100/0.13) as prom_val from contaminantesf 
where id_parameter=="SO2" and value is not null group by  
id_parameter,to_date(from_unixtime(unix_timestamp(`date`,'dd/MM/yyyy HH:mm'))) having prom_val between 191
and 239) as a group by year(a.fech_d) order by anio;
