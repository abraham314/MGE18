from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml import Pipeline

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator  
from timeit import default_timer as timer

#Para las pruebas locales
#sc = SparkContext('local[*]')
#spark = SparkSession(sc)

#Se carga archivo, usamos la carpeta de la tarea pasada para no volver a cargar el archivo
flights = spark.read.csv("s3a://vq-mcd2018/tarea_6/Datos/flights/flights/flights.csv", header=True, inferSchema=True,nullValue = 'null')


#Se seleccionan las variables que usaremos, dejamos fuera las que tienen NA en la mayoria de sus observaciones
data = flights.select(['YEAR', 'MONTH', 'DAY', 'DAY_OF_WEEK', 'AIRLINE', 'FLIGHT_NUMBER', 'TAIL_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'DEPARTURE_DELAY', 'TAXI_OUT', 'WHEELS_OFF', 'SCHEDULED_TIME', 'ELAPSED_TIME', 'AIR_TIME', 'DISTANCE', 'WHEELS_ON', 'TAXI_IN', 'SCHEDULED_ARRIVAL', 'ARRIVAL_TIME', 'ARRIVAL_DELAY', 'DIVERTED', 'CANCELLED'])

data = data.na.drop(subset=['YEAR', 'MONTH', 'DAY', 'DAY_OF_WEEK', 'AIRLINE', 'FLIGHT_NUMBER', 'TAIL_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'DEPARTURE_DELAY', 'TAXI_OUT', 'WHEELS_OFF', 'SCHEDULED_TIME', 'ELAPSED_TIME', 'AIR_TIME', 'DISTANCE', 'WHEELS_ON', 'TAXI_IN', 'SCHEDULED_ARRIVAL', 'ARRIVAL_TIME', 'ARRIVAL_DELAY', 'DIVERTED', 'CANCELLED'])


#Dividimos en entrenamiento y prueba
(trainingData, testData) = data.randomSplit([0.7, 0.3])


#Se hace el String Index para las variables categóricas usando los datos de entrenamiento
airline_indexer = StringIndexer(inputCol='AIRLINE', outputCol='AIRLINE_numeric', handleInvalid='skip').fit(trainingData)

tailNumber_indexer = StringIndexer(inputCol='TAIL_NUMBER', outputCol='TAIL_NUMBER_numeric', handleInvalid='skip').fit(trainingData)

originAirport_indexer = StringIndexer(inputCol='ORIGIN_AIRPORT', outputCol='ORIGIN_AIRPORT_numeric', handleInvalid='skip').fit(trainingData)

destinationAirport_indexer = StringIndexer(inputCol='DESTINATION_AIRPORT', outputCol='DESTINATION_AIRPORT_numeric', handleInvalid='skip').fit(trainingData)

#Hacemos el Vector Assembler, se deja fuera la variable TAIL_NUMBER por la gran cantidad de categorias que tiene
assembler = VectorAssembler(inputCols=['YEAR', 'MONTH', 'DAY', 'DAY_OF_WEEK', 'AIRLINE_numeric', 'FLIGHT_NUMBER', 'ORIGIN_AIRPORT_numeric', 'DESTINATION_AIRPORT_numeric', 'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'TAXI_OUT', 'WHEELS_OFF', 'SCHEDULED_TIME', 'ELAPSED_TIME', 'AIR_TIME', 'DISTANCE', 'WHEELS_ON', 'TAXI_IN', 'SCHEDULED_ARRIVAL', 'ARRIVAL_TIME', 'ARRIVAL_DELAY', 'DIVERTED', 'CANCELLED'], outputCol='features')

#Hacemos grid de los modelos a correr
clfs = {'RF': RandomForestRegressor(labelCol='DEPARTURE_DELAY', featuresCol='features', numTrees=5),
        'GB': GBTRegressor(labelCol='DEPARTURE_DELAY', featuresCol='features', maxIter=5), 
        }

#Hacemos grid de parametros a utilizar por modelo
paramGrid = { 
    'RF': ParamGridBuilder().addGrid(clfs['RF'].maxDepth, [2, 3, 5]).addGrid(clfs['RF'].maxBins, [800, 900,1000]).build(),
    'GB': ParamGridBuilder().addGrid(clfs['GB'].maxDepth, [2, 3, 5]).addGrid(clfs['GB'].maxBins, [800, 900,1000]).build(),
         }

#Modelos a correr
models_to_run = ['RF','GB']

#Definimos nuestra funcion de magic_loop
def magic_loop(models_to_run, clfs, grid, trainingData, testData):
    #Se crea un dataframe para guardar los resultados de RMSE del magic_loop y una lista para los parámetros y mejores modelos
    df = spark.createDataFrame([("a_Modelo0",100.002)], ["Modelo", "RMSE"])
    mejores=list()
    
    #Se hace un for para que recorra todos los modelos a correr
    for index,clf in enumerate([clfs[x] for x in models_to_run]):
        
  
        modelo=clfs[models_to_run[index]]

        #Se define la métrica de evaluacion
        evaluator=RegressionEvaluator(metricName="rmse",labelCol=modelo.getLabelCol(),predictionCol=modelo.getPredictionCol())
	
    	#Se define pasos y pipeline
        stages = [airline_indexer, tailNumber_indexer, originAirport_indexer, destinationAirport_indexer, assembler, modelo]

        pipeline = Pipeline(stages=stages)

        #Se hace cross validation
        crossval = CrossValidator(estimator=pipeline,  
		                 estimatorParamMaps=grid[models_to_run[index]],
		                 evaluator=evaluator,
		                 numFolds=10)
		                 
        #Se hace fit al mejor modelo del cross validation con los datos de entrenamiento
        cvModel = crossval.fit(trainingData)
        
        #Se guarda el mejor modelo y parametros
        mejores.append(cvModel.bestModel)
        
        #Se hacen las predicciones
        predictions = cvModel.transform(testData)
        
	    #Se calcula el RMSE
        rmse = evaluator.evaluate(predictions)

        #Se guarda el RMSE y elmodelo
        df2 = spark.createDataFrame([
	    (models_to_run[index], rmse)
        ], ["Modelo", "RMSE"])

        df = df.union(df2)
    return [mejores,df]

%pyspark
#Se corre magic_loop
resultado,df=magic_loop(models_to_run, clfs, paramGrid, trainingData, testData)


#Vemos los mejores parametros para GB
print(resultado[1].stages[-1]._java_obj.extractParamMap())

#Vemos los mejores parametros para RF
print(resultado[0].stages[-1]._java_obj.extractParamMap())

#Vemos que método fue mejor comparando RMSE
df.orderBy("Modelo", ascending=True).limit(2).show()









