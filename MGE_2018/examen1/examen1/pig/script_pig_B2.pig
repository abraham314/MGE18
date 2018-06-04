datos_cont = load 's3://aws-alex-03032018-metodos-gran-escala/datos/datos_contaminantes_merged.csv' using PigStorage(',') as (date:chararray, id_station:chararray, id_parameter:chararray, value:float, unit:int);
base_estaciones = load 's3://aws-alex-03032018-metodos-gran-escala/datos/estaciones_meteorologicas.csv' using PigStorage(',') as (clave:chararray, nombre:chararray, delegacion_municipio:chararray, entidad:chararray);

datos_cont_sin = FILTER datos_cont BY value IS NOT NULL;
datos_cont_sin2 = FILTER datos_cont_sin BY id_parameter != 'NO' AND id_parameter != 'NOX' AND id_parameter != 'PMCO';

datos_cont_imecas = foreach datos_cont_sin2 generate date,id_station,id_parameter,value,unit,(
    case id_parameter
      when 'CO' then (value*100)/11
      when 'NO2' then (0.001*value*100)/0.21
      when 'SO2' then (0.001*value*100)/0.13
      when 'O3' then (0.001*value*100)/0.21
      when 'PM10' then (value <= 120.0 ? value*(5/6):(value <= 320.0 ? 40+value*0.5:value*(5/8)))
      when 'PM2.5' then (value <= 15.4 ? value*(50/15.4):(value <= 40.4 ? 20.5+value*(49/24.9):(value <= 65.4 ? 21.3+value*(49/24.9):(value <= 150.4 ? 113.2+value*(49/84.9):value*(201/150.5)))))
    end
) as imeca;

cont_alto = order datos_cont_imecas by imeca DESC;
respuesta_b2 = limit cont_alto 1;
respuesta_b2 = join respuesta_b2 by $1, base_estaciones by clave;
respuesta_b2 = foreach respuesta_b2 generate $0 as date, $2 as id_parameter, $3 as value, $6 as id_station, $7 as nombre, $9 as entidad, $5 as imeca;

STORE respuesta_b2 INTO 's3://aws-alex-03032018-metodos-gran-escala/output/pig_b2' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
