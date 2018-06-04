from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession \
    .builder \
    .appName("Protob Conversion to Parquet") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df_2 = spark.read.parquet("s3a://tar7/proto.parquet")

# CUARTO SACA PROMEDIO Y PONE ALIAS PROMEDIO QUE ERA LO QUE LO HACIA FALLAR
promedio = df_2.select(avg(df_2['precio']).alias("promedio"))

# QUINTO GUARDA EL PROMEDIO EN OTRO PARQUET QUE SE LLAMA PROMEDIO
promedio.write.parquet("s3a://tar7/promedio.parquet")
