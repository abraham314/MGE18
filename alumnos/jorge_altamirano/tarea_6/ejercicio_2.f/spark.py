from pyspark.sql.functions import avg, round
from pyspark.sql.window import Window

q2f = flights.select(flights.DAY_OF_WEEK.alias("day"),
        flights.AIRLINE.alias("iata"),
        flights.DEPARTURE_DELAY.alias("delay")).\
    filter(col("day") == 2).\
    groupBy("iata").avg("delay").\
    orderBy(col("avg(delay)").asc()).\
    limit(3).\
    select(\
        "*",
        rank().over(Window().partitionBy().orderBy(col("avg(delay)").asc())).alias("rank")
    ).\
    join(airlines, col("iata") == airlines.IATA_CODE).\
    filter(col("rank") == 3).\
    select("iata", col("AIRLINE").alias("airline"))
    
q2f.show()
q2f.write.csv('s3a://jorge-altamirano/tarea_6/q2f')
