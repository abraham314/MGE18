### Tarea 7

from pyspark.sql import SparkSession 
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import GBTRegressor
import time

## Lee el archivo Tarea 7 

spark = SparkSession.builder.appName('"Tarea_7"').getOrCreate() 

flights = spark.read.csv('s3n://metodosgranescala/tarea7/datos/flights.csv', header = True, inferSchema = True, nullValue = 'null')

## Seleccionando los registros que no tienen nulo DEPARTURE DELAY 
flights = flights.filter("DEPARTURE_DELAY is not NULL")

## Seleccionando los campos relacionados a DEPARTURE_DELAY 
flights = flights.select("MONTH","DAY","DAY_OF_WEEK","AIRLINE","FLIGHT_NUMBER","ORIGIN_AIRPORT","DESTINATION_AIRPORT","SCHEDULED_DEPARTURE","DEPARTURE_TIME","DEPARTURE_DELAY","TAXI_OUT","ELAPSED_TIME","AIR_TIME","DISTANCE","TAXI_IN","SCHEDULED_ARRIVAL","ARRIVAL_TIME","ARRIVAL_DELAY")
flights = flights.na.drop(subset=["MONTH","DAY","DAY_OF_WEEK","AIRLINE","FLIGHT_NUMBER","ORIGIN_AIRPORT","DESTINATION_AIRPORT","SCHEDULED_DEPARTURE","DEPARTURE_TIME","DEPARTURE_DELAY","TAXI_OUT","ELAPSED_TIME","AIR_TIME","DISTANCE","TAXI_IN","SCHEDULED_ARRIVAL","ARRIVAL_TIME","ARRIVAL_DELAY"])

## Transformar las variables de tipo string a una columna con indices por etiqueta

AIRLINE_indexer             = StringIndexer(inputCol='AIRLINE',outputCol='AIRLINE_idx',handleInvalid='skip')
ORIGIN_AIRPORT_indexer      = StringIndexer(inputCol='ORIGIN_AIRPORT',outputCol='ORIGIN_AIRPORT_idx',handleInvalid='skip')
DESTINATION_AIRPORT_indexer = StringIndexer(inputCol='DESTINATION_AIRPORT',outputCol='DESTINATION_AIRPORT_idx',handleInvalid='skip')

## Separamos los conjutos de entrenamiento y prueba

(trainingData, testData) = flights.randomSplit([0.7, 0.3])
      
## Combinamos las columnas en una sola de features 
assembler = VectorAssembler(inputCols=[ "MONTH","DAY","DAY_OF_WEEK","AIRLINE_idx","FLIGHT_NUMBER","ORIGIN_AIRPORT_idx",
                                        "DESTINATION_AIRPORT_idx","SCHEDULED_DEPARTURE","DEPARTURE_TIME","TAXI_OUT",
                                        "ELAPSED_TIME","AIR_TIME","DISTANCE","TAXI_IN","SCHEDULED_ARRIVAL",
                                        "ARRIVAL_TIME","ARRIVAL_DELAY"], outputCol='features')


## Definimos dos modelos a utilizar, un randrom tree y un gradient boost 

rf =  RandomForestRegressor(featuresCol="features",labelCol="DEPARTURE_DELAY",maxBins=1000)
gbt = GBTRegressor(featuresCol="features",labelCol="DEPARTURE_DELAY",maxBins=1000)

param_rf =  ParamGridBuilder().addGrid(rf.maxDepth, [1, 2, 3]).addGrid(rf.numTrees, [5,10]).build()
param_gbt = ParamGridBuilder().addGrid(gbt.maxDepth, [1, 2, 3]).addGrid(gbt.maxIter, [5,10]).build()

modelos =[rf, gbt]
grid = [param_rf, param_gbt]


## Definimos la funcion magic loop para buscar el mejor modelo

def magic_loop(crossFolds,train,test):
    mejor_modelo = []
    metricas = []
    parametros = []    
    for i in [0,1]:
        pipeline = Pipeline(stages=[AIRLINE_indexer, ORIGIN_AIRPORT_indexer, DESTINATION_AIRPORT_indexer, assembler, modelos[i]]) 
        crossValidator = CrossValidator(estimator=pipeline, estimatorParamMaps=grid[i],evaluator=RegressionEvaluator(predictionCol='prediction', labelCol="DEPARTURE_DELAY", metricName='rmse'),numFolds=crossFolds)
        crossValidatorModel = crossValidator.fit(train)
        mejor_modelo.append(crossValidatorModel.bestModel)
        metricas.append(min(crossValidatorModel.avgMetrics))
        parametros.append(grid[i][crossValidatorModel.avgMetrics.index(min(crossValidatorModel.avgMetrics))])
        print('Mejor modelo es:',parametros[metricas.index(min(metricas))]) 
    print('Mejor modelo es:',parametros[metricas.index(min(metricas))]) 
    return(mejor_modelo[metricas.index(min(metricas))])

## Ejecutamos la funcion para buscar el mejor modelo y medimos el tiempo de ejecución

start = time.time()
resultado = magic_loop(10,trainingData,testData)
end = time.time()

print("Tiempo de ejecución (minutos): ", (end-start)/60)
