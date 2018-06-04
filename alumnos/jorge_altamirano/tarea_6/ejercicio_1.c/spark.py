from pyspark.sql.functions import to_timestamp, col, window, datediff
from pyspark.sql.window import Window

delta = orders.\
    select("orderid", \
        to_timestamp("orderdate", "yyyy/MM/dd HH:mm:ss").alias("orderdate"),
        lag("orderdate").over(Window().partitionBy().orderBy(col("orderid").asc())).\
            alias("lag")).\
    select("orderid", "orderdate", "lag", #se separa porque no reconoce lag
        datediff("orderdate", "lag").alias("delta")).\
    orderBy(col("delta").desc())

delta.limit(5).show()
q1c = delta.select(delta.delta).limit(1)
q1c.show()
q1c.write.csv('s3a://jorge-altamirano/tarea_6/q1c')
