datos_cont = load 's3://aws-alex-03032018-metodos-gran-escala/datos/datos_contaminantes_merged.csv' using PigStorage(',') as (date:chararray, id_station:chararray, id_parameter:chararray, value:float, unit:int);
base_estaciones = load 's3://aws-alex-03032018-metodos-gran-escala/datos/estaciones_meteorologicas.csv' using PigStorage(',') as (clave:chararray, nombre:chararray, delegacion_municipio:chararray, entidad:chararray);

datos_cont_sin = FILTER datos_cont BY value IS NOT NULL;
datos_cont_sin3 = foreach datos_cont_sin generate ToDate(SUBSTRING(date,0,10),'dd/MM/yyyy') AS date ,id_station,id_parameter,value,unit ;

estaciones = group datos_cont_sin3 by id_station;
estaciones_ord = foreach estaciones generate group as est, MIN(datos_cont_sin3.date) as fecha_min;
rank_estaciones = order estaciones_ord by fecha_min DESC;
estacion_reciente = limit rank_estaciones 1;
respuesta_b1 = join estacion_reciente by $0, base_estaciones by clave;
respuesta_b1 = foreach respuesta_b1 generate $2 as clave, $3 as nombre, $4 as delegacion_municipio, $5 as entidad, $1 as fecha_ingreso;

STORE respuesta_b1 INTO 's3://aws-alex-03032018-metodos-gran-escala/output/pig_b1' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
