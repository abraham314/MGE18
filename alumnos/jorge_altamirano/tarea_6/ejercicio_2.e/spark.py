from pyspark.sql.functions import window, avg
from pyspark.sql.window import Window

q2e = flights.select(\
        col("DAY_OF_WEEK").alias("day"),
        col("ARRIVAL_DELAY").alias("delay"),
        col("AIRLINE").alias("iata")).\
    groupBy("day", "iata").avg("delay").\
    select("*", col("avg(delay)").alias("delay2")).\
    select(\
        "*",
        rank().over(Window().partitionBy("day").orderBy(col("delay2").desc())).alias("rank")
    ).\
    filter(col("rank") == 1).\
    orderBy("day").\
    join(airlines, col("iata") == airlines.IATA_CODE).\
    select("day", "iata", col("AIRLINE").alias("airline"))

q2e.show()
q2e.write.csv('s3a://jorge-altamirano/tarea_6/q2e')
