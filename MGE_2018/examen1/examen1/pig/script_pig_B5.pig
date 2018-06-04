datos_cont = load 's3://aws-alex-03032018-metodos-gran-escala/datos/datos_contaminantes_merged.csv' using PigStorage(',') as (date:chararray, id_station:chararray, id_parameter:chararray, value:float, unit:int);
datos_cont_sin = FILTER datos_cont BY value IS NOT NULL;
datos_cont_sin2 = foreach datos_cont_sin generate ToDate(SUBSTRING(date,0,10),'dd/MM/yyyy') AS date ,id_station,id_parameter,value,unit ;
datos_2017_NOX = filter datos_cont_sin2 by id_parameter == 'NOX';
datos_NOX_group = group datos_2017_NOX by date;
datos_NOX_max = foreach datos_NOX_group generate group as date, MAX(datos_2017_NOX.value) as valor;
datos_NOX_max = foreach datos_NOX_max generate GetYear(date) as year, ((0.001*valor*100)/0.21) as imeca;
datos_NOX_limite = foreach datos_NOX_max generate year, imeca, ((imeca>=191 AND imeca<=239) ? 1:0) as limite;
datos_NOX_limite_year= group datos_NOX_limite by year;
respuesta_b5 = foreach datos_NOX_limite_year generate group as year, SUM(datos_NOX_limite.limite) as fase1_fase2;

STORE respuesta_b5 INTO 's3://aws-alex-03032018-metodos-gran-escala/output/pig_b5' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
