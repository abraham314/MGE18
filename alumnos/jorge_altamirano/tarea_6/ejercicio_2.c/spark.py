from pyspark.sql.functions import avg, round

q2c = flights.select(col("ARRIVAL_DELAY").alias("delay"), 
        col("DESTINATION_AIRPORT").alias("destination"),
        col("DAY_OF_WEEK").alias("day"),
        col("AIRLINE").alias("airline1")
    ).\
    join(airports, col("destination") == airports.IATA_CODE).\
    filter(col("airport") == "Honolulu International Airport").\
    groupBy("day", "airline1").avg("delay").\
    select("day", "airline1", round(col("avg(delay)"), 2).alias("delay")).\
    orderBy(col("delay").asc()).\
    join(airlines, col("airline1") == airlines.IATA_CODE).\
    select("day", "airline1", "delay", col("airline").alias("airline_name")).\
    limit(1)
    
q2c.show()
q2c.write.csv('s3a://jorge-altamirano/tarea_6/q2c')
