### Tarea 8
### Convierte el archivo csv a formato parquet

import sys
from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName('"Tarea8"').getOrCreate() 

## Recibe como parametros la ruta del archivos de datos y la ruta de salida a parquet 

csvFile     = sys.argv[1]
parquetFile = sys.argv[2]

productos = spark.read.csv(csvFile, header = True, inferSchema = True, nullValue = 'null')
productos.write.parquet(parquetFile,mode="overwrite") 

