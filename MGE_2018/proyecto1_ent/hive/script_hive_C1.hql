select distinct b.id_parameter,b.mes,b.hora,b.value,b.nombre_unidad from (select id_parameter,month(from_unixtime(unix_timestamp(`date`,'dd/MM/yyyy HH:mm'))) as mes,
max(value) as max_val from contaminantesf group by id_parameter,
month(from_unixtime(unix_timestamp(`date`,'dd/MM/yyyy HH:mm')))) as a join (select q.*,month(from_unixtime(unix_timestamp(q.`date`,'dd/MM/yyyy HH:mm'))) as mes,
hour(from_unixtime(unix_timestamp(q.`date`,'dd/MM/yyyy HH:mm'))) as hora, r.nombre_unidad from 
contaminantesf as q join cat_unidades as r on (r.id_unidad=q.unit)) as b on (a.id_parameter=b.id_parameter) and (a.mes=b.mes) and (a.max_val=b.value);
