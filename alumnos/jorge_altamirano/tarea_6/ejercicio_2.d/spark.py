q2d = flights.select(col("DESTINATION_AIRPORT").alias("destination")).\
    groupBy("destination").count().\
    orderBy(col("count").desc()).\
    limit(1).\
    join(airports, col("destination") == airports.IATA_CODE).\
    select("destination", 
        col("AIRPORT").alias("airport"), 
        "count")
        
q2d.show()
q2d.write.csv('s3a://jorge-altamirano/tarea_6/q2d')
