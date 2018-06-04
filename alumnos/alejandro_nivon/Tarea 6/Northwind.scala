import pyspark.sql.functions as psf
from pyspark.sql import Row
from pyspark.sql.functions import *

customers=spark.read.csv('/Volume/northwind/customers.csv', header=True)
employees=spark.read.csv('/Volume/northwind/employees.csv', header=True)
orderDet=spark.read.csv('/Volume/northwind/order_details.csv', header=True)
product=spark.read.csv('/Volume/northwind/products.csv', header=True)
orders=spark.read.csv('/Volume/northwind/orders.csv', header=True)


ej1a1=employees.select('title').filter("title = 'Sales Manager' or title = 'Inside Sales Coordinator' or title = 'Vice President, Sales'").count().show()
d=[{'Conteo': ej1a1}]
spark.createDataFrame(d).show()
spark.createDataFrame(d).rdd.saveAsTextFile("/Volume/Resultados/Northwind/Resej1a1.txt")

ej1a2=employees.select('employeeid', 'firstname', 'lastname', 'title', 'birthdate', 'hiredate', 'city', 'country').filter("title = 'Sales Manager' or title = 'Inside Sales Coordinator' or title = 'Vice President, Sales'")
ej1a2.show()
ej1a2.rdd.saveAsTextFile("/Volume/Resultados/Northwind/Resej1a2.txt")

ej1b1=orders.join(employees, orders.employeeid == employees.employeeid).groupBy(employees.firstname, employees.lastname, employees.title, employees.hiredate).count().sort('count', ascending=False).limit(2).sort('count').limit(1)
ej1b1.show()
ej1b1.rdd.saveAsTextFile("/Volume/Resultados/Northwind/Resej1b1.txt")

ej1b2=orders.join(employees, orders.employeeid == employees.employeeid).groupBy(employees.lastname).count().sort('count', ascending=False).select('count').limit(1)
ej1b2.show()
ej1b2.rdd.saveAsTextFile("/Volume/Resultados/Northwind/Resej1b2.txt")

orders.select(to_date('orderdate'))
tab = orders.select(to_date('orderdate').alias('fecha')).withColumn("id", psf.monotonically_increasing_id())
lag=[Row(idLAG=int(i[0])+1) for i in tab1.select('id').collect()]
lagCol = spark.createDataFrame(lag).withColumn('row_index', psf.monotonically_increasing_id())
tab = tab.join(lagCol, tab.id == lagCol.row_index).select('fecha', 'id', 'idLAG')
tab = tab.select(col('id'), col('fecha').alias('fecha1')).join(tab.select(col('idLAG'), col('fecha').alias('fecha2')), tab.id == tab.idLAG).sort('id', ascending=True)
tab = tab.withColumn('diff', datediff(tab['fecha1'], tab['fecha2']))
ej1c = tab.agg({'diff': 'max'})
ej1c.show()
ej1c.rdd.saveAsTextFile("/Volume/Resultados/Northwind/Resej1c.txt")