SELECT count(DISTINCT(reportsto))
from employees
WHERE reportsto > 0;
