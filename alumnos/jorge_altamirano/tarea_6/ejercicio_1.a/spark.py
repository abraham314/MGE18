reports = employees.select("reportsto").filter(employees.reportsto > 0).distinct()
print("Reporta a %d jefes."%reports.count())
reports = [int(i.reportsto) for i in reports.collect()]
q1a = employees.select("employeeid", "firstname", "lastname", "title", "birthdate", "hiredate", "city", "country").\
    filter(employees.employeeid.isin(reports))
q1a.write.csv('s3a://jorge-altamirano/tarea_6/q1a')
q1a.show()
