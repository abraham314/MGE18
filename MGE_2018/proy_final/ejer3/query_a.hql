with tab as (select descrip, count(*) as num,  rank() over (ORDER BY count(*) DESC) as ranke 
from (select a.causa_def,b.descrip from  defun_2016 as a join decatcausa as b on a.causa_def = b.`?cve`)t  group by descrip) 

select*from tab where ranke=34
