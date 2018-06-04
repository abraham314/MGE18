with chi as (select causa, count(*) as num,rank() over (ORDER BY count(*) DESC) as ranke from  
(select a.ent_ocurr,b.nom_loc as edo,c.descrip as genero,d.descrip as causa from  defun_2016 as a join 
(select distinct cve_ent, nom_loc from decateml2 where cve_mun="000") as b on  
cast(trim(a.ent_ocurr) as float)=cast(trim(b.cve_ent) as float) join 
desexo as c on a.sexo=c.`?cve` join decatcausa as d on  a.causa_def=d.`?cve`)t 
where edo="Chihuahua" and genero="Mujeres" group by causa)  

select*from chi where ranke=7 

