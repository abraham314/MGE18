from pyspark.sql.functions import col
from pyspark.sql import Row

n_order = orders.groupBy("employeeid").count().sort(col("count").desc()).collect()
#orders.groupBy("employeeid").count().sort(col("count").desc()).show()
best = sc.parallelize([Row(employeeid = n_order[1][0], n_orders = n_order[1][1], best_n_orders = n_order[0][1])]).toDF()
#best.show()
q1b = employees.filter(employees.employeeid == n_order[1][0]).\
    join(best, employees.employeeid == best.employeeid).\
    select("firstname", "lastname", "title", "hiredate", "n_orders", "best_n_orders")
q1b.write.csv('s3a://jorge-altamirano/tarea_6/q1b')
q1b.show()
