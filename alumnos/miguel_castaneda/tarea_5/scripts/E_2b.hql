use flights;

select distinct substring(lpad(scheduled_departure,4,"0"),1,2) as hora 
from flights f where f.destination_airport ='HNL' and f.origin_airport ='SFO';