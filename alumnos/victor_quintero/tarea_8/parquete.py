#s3://larrazolo/scripts/parquete.py

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

sc = SparkContext()
spark = SparkSession(sc)

esquema = StructType().add("producto", StringType(), True).add("presentacion", StringType(), True).add("marca", StringType(), True).add("categoria", StringType(), True).add("catalogo", StringType(), True).add("precio", DoubleType(), True).add("fechaRegistro", TimestampType(), True).add("cadenaComercial", StringType(), True).add("giro", StringType(), True).add("nombreComercial", StringType(), True).add("direccion", StringType(), True).add("estado", StringType(), True).add("municipio", StringType(), True).add("latitud", StringType(), True).add("longitud", StringType(), True)

df = spark.read.csv("s3://vq-mcd2018/Tarea_8/prfeco.csv", #ubicacion exacta de datos en S3 
                        header = True,
                        inferSchema=False,
                        schema= esquema,
                        timestampFormat="yyyy-MM-dd hh:mm:ss")
                        
df.write.parquet("s3://vq-mcd2018/Tarea_8/parquete") #ubicacion donde se creara carpeta que contenga .parquet
