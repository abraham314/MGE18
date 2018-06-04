q2g = flights.select(
        flights.ORIGIN_AIRPORT.alias("origin"),
        flights.DESTINATION_AIRPORT.alias("destination")).\
    #groupBy("origin", "destination").count().alias("origin_count").\
    groupBy("destination").count().\
    orderBy(col("count").desc()).\
    limit(1).\
    join(airports, col("destination") == airports.IATA_CODE).\
    select("destination", 
        col("AIRPORT").alias("airport"))
q2g.show()
q2g.write.csv('s3a://jorge-altamirano/tarea_6/q2g')
