from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext(appName="parquet")
spark = SparkSession(sc)

schema = StructType().add("producto", StringType(), True)\
                     .add("presentacion", StringType(), True)\
                     .add("marca", StringType(), True)\
                     .add("categoria", StringType(), True)\
                     .add("catalogo", StringType(), True)\
                     .add("precio", DoubleType(), True)\
                     .add("fechaRegistro", TimestampType(), True)\
                     .add("cadenaComercial", StringType(), True)\
                     .add("giro", StringType(), True)\
                     .add("nombreComercial", StringType(), True)\
                     .add("direccion", StringType(), True)\
                     .add("estado", StringType(), True)\
                     .add("municipio", StringType(), True)\
                     .add("latitud", StringType(), True)\
                     .add("longitud", StringType(), True)

datos = spark.read.csv("s3a://al102964-bucket1/tarea8/all_data.csv",                        
                        inferSchema=False,
                        schema = schema,
                        header = True,
                        timestampFormat="yyyy-MM-dd hh:mm:ss")
                        
datos.write.parquet("s3a://al102964-bucket1/tarea8/parquet")
