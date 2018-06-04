from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

df = spark.read.parquet("s3://vq-mcd2018/Tarea_8/parquete") #carpeta que se creo en el parquete.py donde se almaceno el .parquet

df.select("producto").groupBy("producto").count().alias("Conteo").coalesce(1).write.format('json').save("s3://vq-mcd2018/Tarea_8/resultado") #Carpeta que vas a crear para depositar el resultado


