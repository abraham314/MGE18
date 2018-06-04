SELECT count(DISTINCT(reportsto))
FROM employees
WHERE reportsto > 0;

