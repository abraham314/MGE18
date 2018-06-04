datos_cont = load 's3://aws-alex-03032018-metodos-gran-escala/datos/datos_contaminantes_merged.csv' using PigStorage(',') as (date:chararray, id_station:chararray, id_parameter:chararray, value:float, unit:int);
base_estaciones = load 's3://aws-alex-03032018-metodos-gran-escala/datos/estaciones_meteorologicas.csv' using PigStorage(',') as (clave:chararray, nombre:chararray, delegacion_municipio:chararray, entidad:chararray);

datos_cont_sin = FILTER datos_cont BY value IS NOT NULL;
datos_cont_sin2 = foreach datos_cont_sin generate ToDate(SUBSTRING(date,0,10),'dd/MM/yyyy') AS date ,id_station,id_parameter,value,unit ;

est_dia = group datos_cont_sin2 by (id_station,date);
est_dia_count = foreach est_dia generate group as est, COUNT(datos_cont_sin2.value) as obs_dia;
est_dia_count_sep = foreach est_dia_count generate flatten(est) as (id_est,dia),obs_dia;
est_group = group est_dia_count_sep by id_est;
est_group_prom = foreach est_group generate group as est ,COUNT(est_dia_count_sep.dia) as num_dias, SUM(est_dia_count_sep.obs_dia) as num_obs;
respuesta_b4 = foreach est_group_prom generate est,num_dias,num_obs,ROUND((num_obs/num_dias)) as promedio_mediciones;
respuesta_b4 = join respuesta_b4 by $0, base_estaciones by clave;
respuesta_b4 = foreach respuesta_b4 generate $0 as estacion,$5 as nombre, $1 as num_dias, $2 as num_obs, $3 as promedio_mediciones;

STORE respuesta_b4 INTO 's3://aws-alex-03032018-metodos-gran-escala/output/pig_b4' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
