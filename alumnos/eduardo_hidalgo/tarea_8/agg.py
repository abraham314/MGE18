import sys
import os
import datetime
from pyspark.sql import *
from pyspark.sql import functions
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName='agg')

    spark = SQLContext(sc)
    s3_path = str(sys.argv[1])
    # Lectura de los datos
    df = spark.read.parquet(s3_path+'/profeco/profeco_gzip')

    resumen = df.select('producto', 'precio').groupBy('producto').agg(functions.count('producto').alias("Count"),functions.mean("precio").alias("precio medio"))

    # Hacemos coalesce para que tengamos un solo archivo de salida para estos
    resumen.coalesce(1).write.csv(s3_path+'/profeco/agg',mode='OVERWRITE', header=True)

    sc.stop()


