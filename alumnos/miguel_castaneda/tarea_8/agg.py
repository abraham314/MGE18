### agg.py
import sys
from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName('"Tarea8"').getOrCreate() 

parquetFile = sys.argv[1]
salidaFile  = sys.argv[2]

### Lee parquet
productos = spark.read.parquet(parquetFile) 

### Agrupa 
total_productos = productos.groupBy("producto","marca","categoria").count()
total_productos.write.parquet(salidaFile,mode="overwrite") 






