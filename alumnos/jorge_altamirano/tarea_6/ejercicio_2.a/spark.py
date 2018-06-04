from pyspark.sql.functions import col

q2a = flights.select("DESTINATION_AIRPORT", "AIRLINE").\
    join(airports, flights.DESTINATION_AIRPORT == airports.IATA_CODE).\
    select(col("DESTINATION_AIRPORT").alias("iata_airport"), 
        col("AIRPORT").alias("airport"),
        col("AIRLINE").alias("airlines")).\
    join(airlines, col("airlines") == airlines.IATA_CODE).\
    select(col("IATA_CODE").alias("iata"),
        col("AIRLINE").alias("airline")).\
    filter(col("airport") == "Honolulu International Airport").\
    distinct()
q2a.show()
q2a.write.csv('s3a://jorge-altamirano/tarea_6/q2a')
