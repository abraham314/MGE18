# Importamos las librerias necesarias para nuestro análisis descriptivo
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

# Lectura de datos
allDatainput= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("s3:/luigiclasemge/all_data.csv")
#Parqueteo
allDatainput.write.parquet('s3://luigiclasemge/output.parquet')
