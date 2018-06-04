select id_parameter,
to_date(from_unixtime(unix_timestamp(`date`,'dd/MM/yyyy HH:mm'))) as fech_d,avg(value) as prom_val from contaminantes 
where id_parameter=="PM10" and value is not null group by  
id_parameter,to_date(from_unixtime(unix_timestamp(`date`,'dd/MM/yyyy HH:mm'))) having prom_val between 121
and 220 order by fech_d;
