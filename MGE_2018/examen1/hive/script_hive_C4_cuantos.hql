select count(*) as malos from(select id_parameter,
to_date(from_unixtime(unix_timestamp(`date`,'dd/MM/yyyy HH:mm'))) as fech_d,avg(value) as prom_val from contaminantes 
where id_parameter=="PM2.5" and value is not null group by  
id_parameter,to_date(from_unixtime(unix_timestamp(`date`,'dd/MM/yyyy HH:mm'))) having prom_val between 40.5
and 65.4) as a;
