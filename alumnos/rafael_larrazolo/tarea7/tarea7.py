%pyspark

# IMPORTAMOS LIBRERÍAS NECESARIAS PARA CORRER EL CÓDIGO
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.context import SparkContext
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import DecisionTreeRegressor

# SE HACE LA LECTURA DE LOS DATOS, INCLUYENDO LOS ENCABEZADOS
flights = spark.read.csv("s3a://larrazolo/data/flights.csv", header = True, inferSchema = True, nullValue = 'null')

# SE HACE UN SELECT PARA NO TENER LAS VARIABLES QUE INDICAN EL MOTIVO DEL RETRASO.
data = flights.select(['YEAR', 'MONTH', 'DAY', 'DAY_OF_WEEK', 'AIRLINE', 'FLIGHT_NUMBER', 'TAIL_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'DEPARTURE_DELAY', 'TAXI_OUT', 'WHEELS_OFF', 'SCHEDULED_TIME', 'ELAPSED_TIME', 'AIR_TIME', 'DISTANCE', 'WHEELS_ON', 'TAXI_IN', 'SCHEDULED_ARRIVAL', 'ARRIVAL_TIME', 'ARRIVAL_DELAY', 'DIVERTED', 'CANCELLED'])
# SE ELIMINAN LAS OBSERVACIONES QUE TIENE ALGÚN 'NA'.
data = data.na.drop(subset=['YEAR', 'MONTH', 'DAY', 'DAY_OF_WEEK', 'AIRLINE', 'FLIGHT_NUMBER', 'TAIL_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'DEPARTURE_DELAY', 'TAXI_OUT', 'WHEELS_OFF', 'SCHEDULED_TIME', 'ELAPSED_TIME', 'AIR_TIME', 'DISTANCE', 'WHEELS_ON', 'TAXI_IN', 'SCHEDULED_ARRIVAL', 'ARRIVAL_TIME', 'ARRIVAL_DELAY', 'DIVERTED', 'CANCELLED'])
# CAMBIAMOS EL NOMBRE DE LA VARIABLE 'DEPARTURE_DELAY' A 'label' YA QUE EN MIS PIPELINE NECESITABA TENER ESE NOMBRE LA VARIABLE A PRONOSTICAR.
data = data.withColumnRenamed('DEPARTURE_DELAY', 'label')

# SE SEPARAN LOS DATOS EN ENTRENAMIENTO Y PRUEBA.
(training_data, test_data) = data.randomSplit([0.7, 0.3])
# CONTAMOS EL NÚMERO DE OBSERVACIONES EN EL COONJUNTO DE ENTRENAMIENTO.
training_data.count()


# SE UTILIZAN LAS CLASES NECESARIAS PARA INTRODUCIRLAS AL PIPELINE. ÉSTAS PERMITIRÁN TRABAJAR CON LAS VARIABLES
# CATEGÓRICAS. SE UTILIZAN CUATRO, UNA PARA CADA VARIABLE DE TIPO CATEGÓRICA.
airline_indexer = StringIndexer(inputCol='AIRLINE', outputCol='AIRLINE_numeric', handleInvalid='skip')
tailNumber_indexer = StringIndexer(inputCol='TAIL_NUMBER', outputCol='TAIL_NUMBER_numeric', handleInvalid='skip')
originAirport_indexer = StringIndexer(inputCol='ORIGIN_AIRPORT', outputCol='ORIGIN_AIRPORT_numeric', handleInvalid='skip')
destinationAirport_indexer = StringIndexer(inputCol='DESTINATION_AIRPORT', outputCol='DESTINATION_AIRPORT_numeric', handleInvalid='skip')

# EL ENSAMBLADOR TAMBIÉN ENTRA AL PIPELINE EL CUAL SIRVE PARA 'JUNTAR' LOS FEATURES NECESARIOS COMO INPUT PARA EL OBJETO MODELO.
assembler = VectorAssembler(inputCols=['YEAR', 'MONTH', 'DAY', 'DAY_OF_WEEK', 'AIRLINE_numeric', 'FLIGHT_NUMBER', 'ORIGIN_AIRPORT_numeric', 'DESTINATION_AIRPORT_numeric', 'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'TAXI_OUT', 'WHEELS_OFF', 'SCHEDULED_TIME', 'ELAPSED_TIME', 'AIR_TIME', 'DISTANCE', 'WHEELS_ON', 'TAXI_IN', 'SCHEDULED_ARRIVAL', 'ARRIVAL_TIME', 'ARRIVAL_DELAY', 'DIVERTED', 'CANCELLED'], outputCol='features')

# DECISION TREE

# SE CREA EL OBJETO QUE ES EL MODELO "DECISION TREE"
dt = DecisionTreeRegressor(labelCol="label", featuresCol="features", maxBins=800)

# SE CREA EL PIPELINE CORRESPONDIENTE, EL CUAL CONTIENE LAS CLASES QUE MODIFICAN LAS VARIABLES CATEGÓRICAS, ENSAMBLA TODOS LOS FEATURES Y PROPONE EL MODELO DECISION TREE.
pipeline_dt = Pipeline(stages=[airline_indexer, tailNumber_indexer, originAirport_indexer, destinationAirport_indexer, assembler, dt])

# SE CONSTRUYE LA REJILLA CON LOS PARÁMETROS A PROBAR PARA EL DECISION TREE.
paramGrid_dt = ParamGridBuilder() \
    .addGrid(dt.maxDepth, [2, 5, 10]) \
    .addGrid(dt.minInstancesPerNode, [100, 500, 1000]) \
    .build()


# RANDOM TREE

# SE CREA EL OBJETO QUE ES MODELO "RANDOM TREE"
rf = RandomForestRegressor(labelCol='label', featuresCol='features', maxBins=800)

# SE CREA EL PIPELINE CORRESPONDIENTE, EL CUAL CONTINE LAS CLASES QUE MODIFICAN LAS VARIABLES CATEGÓRICAS, ENSAMBLA TODOS LOS FEATURES Y PROPONE EL MODELO RANDOM FOREST.
pipeline_rf = Pipeline(stages=[airline_indexer, tailNumber_indexer, originAirport_indexer, destinationAirport_indexer, assembler, rf])

# SE CONSTRUYE LA REJILLA CON LOS PARÁMETROS A PROBAR PARA EL RANDOM FOREST.
paramGrid_rf = ParamGridBuilder() \
    .addGrid(rf.maxDepth, [2, 3, 4]) \
    .addGrid(rf.numTrees, [10, 12, 15]) \
    .build()

# SE CREAN LISTAS CON LOS MODELOS Y REJILLAS QUE SERÁN INPUT DEL MAGICLOOP.
my_models = [rf, dt]
my_grids = [paramGrid_rf, paramGrid_dt]


# SE DEFINE EL MAGIC LOOP
def magic_loop(models_to_run, parameter_grids, data_train, data_test, cross_validation_folds):

    best_models = list() 	#Lista donde se depositará el mejor ajuste de cada modelo
    errors = list()			# Lista donde se depositará el RMSE de prueba para cada mejor modelo
    
    for model in models_to_run:					# Se corre para cada modelo (en este caso DecisionTree y RandomForest)
        
        pipeline = Pipeline(stages=[airline_indexer, tailNumber_indexer, originAirport_indexer, destinationAirport_indexer, assembler, model])			# Se crea el pipeline con la modificación de categóricas y agregando el modelo a utilizar
        
        grid =  parameter_grids[models_to_run.index(model)]			# Se toma la rejilla de parámetros correspondiente
    
        crossval = CrossValidator(estimator = pipeline,				# Se crea el objeto para la validación cruzada con k=10
                              estimatorParamMaps = grid,
                              evaluator = RegressionEvaluator(),
                              numFolds= cross_validation_folds)
    
        cvModel = crossval.fit(data_train)							# Se ajusta con los datos de entrenamiento
        winner = cvModel.best_models 								# Se obtiene el mejor conjunto de parámetros dado el grid
        best_models.append(winner)									# Lo incluímos en la lista definida al inicio
        predictions = cvModel.transform(data_test)					# Se ocupa el conjunto de prueba para hacer el pronóstico
        evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)						# Se define el evaluador y se calcula el error de prueba
        errors.append(rmse)											# Se agrega el error obtenido en la lista
        
    ganador_index = errors.index(min(errors))						# Buscamos el error mínimo de TODOS los modelos (Tree y Forest)
    
    #print("Best Model: " % best_models[ganador_index])
    #print("Root Mean Squared Error (RMSE) on test data for best model = %g" % errors[ganador_index])
    return(best_models ,best_models[ganador_index] , errors[ganador_index])   #Obtenemos la lista que contiene al mejor DT, RF y el mejor modelo overall y su error.


salida = magic_loop(models_to_run=my_models, parameter_grids=my_grids, data_train=training_data, data_test=test_data, cross_validation_folds=10)
print("El mejor modelo fue {} con un RMSE de {} ".format(salida[1].stages[5], salida[2]))


# SE MIDE EL TIEMPO DE EJECUCIÓN    
import timeit, functools
t = timeit.Timer(functools.partial(magic_loop(models_to_run=my_models, parameter_grids=my_grids, data_train=training_data, data_test=test_data, cross_validation_folds=10)) 

t.timeit()
