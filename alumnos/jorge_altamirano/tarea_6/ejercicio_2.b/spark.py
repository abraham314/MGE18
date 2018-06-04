from pyspark.sql.functions import floor

q2b = flights.select(col("DESTINATION_AIRPORT").alias("destination"),
        col("ORIGIN_AIRPORT").alias("origin"),
        col("SCHEDULED_DEPARTURE").alias("departure_time")).\
    join(airports, col("origin") == col("IATA_CODE")).\
    select("destination", "origin", "departure_time",
        col("AIRPORT").alias("origin2")).\
    join(airports, col("destination") == col("IATA_CODE")).\
    select("departure_time", "destination",  
        col("AIRPORT").alias("destination2"),
        "origin", "origin2").\
    filter((col("origin") == "SFO") & \
        (col("destination2") == "Honolulu International Airport")).\
    select("departure_time").\
    orderBy("departure_time").\
    select(floor(col("departure_time") / 100).alias("departs")).\
    distinct().\
    orderBy(col("departs"))

q2b.show()
q2b.write.csv('s3a://jorge-altamirano/tarea_6/q2b')
