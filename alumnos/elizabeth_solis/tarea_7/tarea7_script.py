
## El código que se muestra a continuación fue corrido en AWS, en particular en Zeppelin
## La evidencia se encuentra en la carpeta images y está descrito en el RMD y html
## Aquí se incluye el código con comentarios

#Primero se incluyen las librerias necesarias
from pyspark import SparkConf, SparkContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer, StringIndexer, VectorAssembler
from pyspark.ml.regression import GeneralizedLinearRegression, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

#mostramos la configuracion completa de spark para aseguranos de que todo este correcto
spark
sc._conf.getAll()

#Cargamos los datos de el archivo que tenemos en S3 (la siguiente instrucción solo funcionará en AWS)
allData= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("s3://ex1oregon/fligths_files/flights.csv")

# Acomodamos los datos un poco para comenzar el enginering
allData= allData.drop("CANCELLATION_REASON","AIR_SYSTEM_DELAY","SECURITY_DELAY","AIRLINE_DELAY","LATE_AIRCRAFT_DELAY","WEATHER_DELAY")
allData= allData.na.drop()
allData= allData.withColumnRenamed("DEPARTURE_DELAY","label")
allData

#Generamos los indexer para poder procesar la informacion dentro del pipeline
indexerAirline = StringIndexer(inputCol="AIRLINE", outputCol="AIRLINE_INDEX").fit(allData)
indexerAirport = StringIndexer(inputCol="ORIGIN_AIRPORT", outputCol="ORIGIN_AIRPORT_INDEX").fit(allData)
indexerAirportDest = StringIndexer(inputCol="DESTINATION_AIRPORT", outputCol="DESTINATION_AIRPORT_INDEX").fit(allData)
indexerTail = StringIndexer(inputCol="TAIL_NUMBER", outputCol="TAIL_NUMBER_INDEX").fit(allData)   

# Incluimos el proceso para hacer la conjuncion de los features para Spark ML
assembler = VectorAssembler(inputCols=["YEAR", "MONTH", "DAY","DAY_OF_WEEK","FLIGHT_NUMBER","SCHEDULED_DEPARTURE","DEPARTURE_TIME",
               "TAXI_OUT","WHEELS_OFF","SCHEDULED_TIME","ELAPSED_TIME","AIR_TIME","DISTANCE","WHEELS_ON","TAXI_IN",
               "SCHEDULED_ARRIVAL","ARRIVAL_TIME","ARRIVAL_DELAY","DIVERTED","CANCELLED","AIRLINE_INDEX",
               "ORIGIN_AIRPORT_INDEX","DESTINATION_AIRPORT_INDEX","TAIL_NUMBER_INDEX"],
    outputCol="features")


# Declaramos los modelos de regresion y los parametros a probar 
# dentro del pipeline, y despues generamos las listas de pipelines y parametros para ser procesados por el magic loop.
lr = LinearRegression()
lrParamGrid = ParamGridBuilder()\
    .addGrid(lr.regParam, [0.1, 0.05, 0.01]) \
    .addGrid(lr.maxIter, [5, 6, 10])\
    .build()

glr = GeneralizedLinearRegression(family="gaussian")
glrParamGrid = ParamGridBuilder()\
    .addGrid(glr.regParam, [0.4, 0.5, 0.01]) \
    .addGrid(glr.maxIter, [3, 7, 12])\
    .build()

glr2 = GeneralizedLinearRegression(family="Tweedie")
glr2ParamGrid = ParamGridBuilder()\
    .addGrid(glr2.regParam, [0.001, 0.23, 0.89]) \
    .addGrid(glr2.maxIter, [2, 4, 11])\
    .build()

lrpipeline = Pipeline(stages=[indexerAirline, indexerAirport, indexerAirportDest, indexerTail,assembler, lr])
glrpipeline = Pipeline(stages=[indexerAirline, indexerAirport, indexerAirportDest, indexerTail,assembler, glr])
glr2pipeline = Pipeline(stages=[indexerAirline, indexerAirport, indexerAirportDest, indexerTail,assembler, glr2])

pipelines = [lrpipeline, glrpipeline, glr2pipeline]
gridParams = [lrParamGrid, glrParamGrid, glr2ParamGrid]


# Declaración de los parametros del entrenamiento, el numero de cross validations, 
#el porcentaje de los sets de datos y las semilla para partir los datos.
crossNumFolds=10
trainingPercent = 0.7
testPercent = 0.3
randomSeed = 0


# Con estas instrucciones partimos los sets de entrenamiento y de prueba
trainingData, testData = allData.randomSplit([trainingPercent,testPercent], seed=randomSeed) # need to ensure same split for each time
trainingData

# A continuacion, esta declarado el magicloop para evaluar todos los modelos, con sus respectivos hiper-parametros
bestModels = []
def magic_loop_pipe():
    i = 0
    for pipeline in pipelines:
        paramGrid = gridParams[i]
        crossval = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=RegressionEvaluator(), numFolds=crossNumFolds) 
        #este fit automaticamente regresa el mejor model del cross validation
        model = crossval.fit(trainingData)
        i = i +1
        bestModels.append(model)

#Con el siguiente bloque de codigo ejecutamoms el magic loop declarado en el paso anterior.
magic_loop_pipe()


# Una vez ejecutado el magic loop, nuestro resultado es una lista de modelos con los 
# mejores resultados para cada uno de ellos. Con el Siguiente codigo, mostramos los hiper-parametros de los mejores modelos.
for model in bestModels:
    topModel = model.bestModel.stages[5]._java_obj#using bestmodel
    print("{}  regParam = {}  MaxIter = {}".format(model.bestModel.stages[5], topModel.getRegParam(),topModel.getMaxIter()))


from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.linalg import DenseVector
testTransformedData = indexerAirline.transform(testData)
testTransformedData = indexerAirport.transform(testTransformedData)
testTransformedData = indexerAirportDest.transform(testTransformedData)
testTransformedData = indexerTail.transform(testTransformedData)
testTransformedData = assembler.transform(testTransformedData)

# Obtención de métricas para evaluar las predicciones con base en cada uno de los modelos probados
# Metricas: MSE, RMSE, MAE, Variance (Explained), R-squared
for model in bestModels:
    topModel = model.bestModel.stages[5]
    predicts = topModel.transform(testTransformedData)
    predicts = predicts.rdd.map(lambda p:(float(p.label),float( p.prediction)))
    metrics =  RegressionMetrics(predicts)
    # Squared Error
    print("MSE = %s" % metrics.meanSquaredError)
    print("RMSE = %s" % metrics.rootMeanSquaredError)
    # R-squared
    print("R-squared = %s" % metrics.r2)
    # Mean absolute error
    print("MAE = %s" % metrics.meanAbsoluteError)
    # Explained variance
    print("Explained variance = %s" % metrics.explainedVariance)








































