defun = load 's3://aws-alex-03032018-metodos-gran-escala/datos/defun_2016.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') as (ent_regis:chararray,mun_regis:chararray,ent_resid:chararray,mun_resid:chararray,tloc_resid:int,loc_resid:chararray,ent_ocurr:chararray,mun_ocurr:chararray,tloc_ocurr:int,loc_ocurr:chararray,causa_def:chararray,lista_mex:chararray,sexo:int,edad:int,dia_ocurr:int,mes_ocurr:int,anio_ocur:int,dia_regis:int,mes_regis:int,anio_regis:int,dia_nacim:int,mes_nacim:int,anio_nacim:int,ocupacion:int,escolarida:int,edo_civil:int,presunto:int,ocurr_trab:int,lugar_ocur:int,necropsia:int,asist_medi:int,sitio_ocur:int,cond_cert:int,nacionalid:int,derechohab:int,embarazo:int,rel_emba:int,horas:int,minutos:int,capitulo:int,grupo:int,lista1:chararray,gr_lismex:chararray,vio_fami:int,area_ur:int,edad_agru:chararray,complicaro:int,dia_cert:int,mes_cert:int,anio_cert:int,maternas:chararray,lengua:int,cond_act:int,par_agre:int,ent_ocules:chararray,mun_ocules:chararray,loc_ocules:chararray,razon_m:int,dis_re_oax:chararray);
decateml = load 's3://aws-alex-03032018-metodos-gran-escala/datos/decateml.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') as (cve_ent:chararray,cve_mun:chararray,cve_loc:chararray,nom_loc:chararray);

ent_residencia = group defun by ent_resid;
residencia_def = foreach ent_residencia generate group as ent_resid,COUNT($1) as num_def;
decateml_est = filter decateml by cve_mun=='000\t' and cve_loc=='0000\t';
join_resid = join decateml_est by cve_ent,residencia_def by ent_resid;
join_resid_ord = order join_resid by num_def DESC;
residencia_limpios = foreach join_resid_ord generate cve_ent as Clave,nom_loc as Entidad,num_def as Total;

STORE residencia_limpios INTO 's3://aws-alex-03032018-metodos-gran-escala/output/pig_b_aws' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');
