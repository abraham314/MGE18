#!/usr/bin/env python3
#importo librerías
import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import *
from pyspark.sql import DataFrameStatFunctions, DataFrame
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import *
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import re as re
import time

#arranque de Spark
conf = SparkConf()
conf.set("spark.driver.memory", "16g")
conf.set("spark.driver.cores", 4)
conf.set("spark.driver.memoryOverhead", 0.9)
conf.set("spark.executor.memory", "32g")
conf.set("spark.executor.cores", 12)
conf.set("spark.jars", "/home/jaa6766")
sc = SparkContext(master = "local[14]", sparkHome="/usr/local/spark/", 
                  appName="tarea-mge-7", conf=conf)
spark = SQLContext(sc)
##############
#carga de datos
flights = spark.read.csv("hdfs://172.17.0.2:9000/data/flights/flights.csv", 
                         inferSchema=True, 
                         header=True)
flights = flights.cache()
flights.show(2)
flights.printSchema()
####################
#feature engineering
#columnas más relevantes
relevant_cols = ["YEAR", "MONTH", "DAY", "DAY_OF_WEEK", "AIRLINE", 
                "FLIGHT_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT",
                "SCHEDULED_DEPARTURE", "DEPARTURE_TIME", "SCHEDULED_TIME",
                "AIR_TIME", "DISTANCE", "SCHEDULED_ARRIVAL", "CANCELLED",
                "DEPARTURE_DELAY"]
relevant_cols

### selección de columnas
flights = flights.select(relevant_cols)
### descartar nulos
flights = flights.na.drop().cache()
### dividir en sets de entrenamiento y set de pruebas
flights.write.parquet("hdfs://172.17.0.2:9000/tmp/flights-tmp.parquet", mode="overwrite")
flights.show(2)

#por eficiencia he visto que es mejor guardarlo
flights = spark.read.parquet("hdfs://172.17.0.2:9000/tmp/flights-tmp.parquet")
flights = flights.cache()
#dividir el set
(train, test) = flights.randomSplit([0.7, 0.3], seed=175904)
train2 = train.sample(fraction=0.001, seed=175904)

#guardamos por eficiencia, nuevamente
train.write.parquet("hdfs://172.17.0.2:9000/tmp/train-tmp.parquet", mode="overwrite")
test.write.parquet("hdfs://172.17.0.2:9000/tmp/test-tmp.parquet", mode="overwrite")
train = spark.read.parquet("hdfs://172.17.0.2:9000/tmp/train-tmp.parquet")
test = spark.read.parquet("hdfs://172.17.0.2:9000/tmp/test-tmp.parquet")
train = train.cache()
test = test.cache()

#descartando categóricas y la que vamos a predecir: "DEPARTURE_DELAY"
features = [ x for x in train.schema.names if x not in 
            [ "DEPARTURE_DELAY", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"] ]
#agregando las columnas que ya van a ser indizadas
features.append("airline_indexer")
features.append("origin_indexer")
features.append("destination_indexer")
print("Features que se tomarán para la predicción: ", features)

#indización de columnas: de categóricas a texto
li0 = StringIndexer(inputCol="AIRLINE",
                    outputCol="airline_indexer", 
                    handleInvalid="skip") \
    .fit(flights)
li1 = StringIndexer(inputCol="ORIGIN_AIRPORT",
                    outputCol="origin_indexer", 
                    handleInvalid="skip") \
    .fit(flights)
li2 = StringIndexer(inputCol="DESTINATION_AIRPORT",
                    outputCol="destination_indexer", 
                    handleInvalid="skip") \
    .fit(flights)
li  = StringIndexer(inputCol="DEPARTURE_DELAY",
                    outputCol="delay_indexer", 
                    handleInvalid="skip") \
    .fit(flights)
#este ensamble es requerido para usar los clasificadores y regresores de Spark ML
va0 = VectorAssembler() \
    .setInputCols(features) \
    .setOutputCol("features")
pca = PCA(k=10, inputCol="features", outputCol="features_pca")
lc = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                   labels=li.labels)
######################
# modelos
# regresión logística
lr = LogisticRegression(featuresCol="features_pca",
                        labelCol="delay_indexer",
                        #maxIter=10, elasticNetParam=0.8,
                        regParam=0.3, 
                        family="multinomial",
                        predictionCol="prediction")
#creación del pipeline, este no se va a utilizar en el Magic Loop
pipeline1 = Pipeline(stages=[li0, li1, li2, va0, pca, li, lr, lc]) 
# regresión lineal
glr = GeneralizedLinearRegression(featuresCol="features_pca",
                        labelCol="delay_indexer", family="Gaussian")
#creación del pipeline, este no se va a utilizar en el Magic Loop
pipeline2 = Pipeline(stages=[li0, li1, li2, va0, pca, li, glr, lc])

#####################
# Magic Loop
def magic_loop3(pipelines, grid, train, test, cvfolds=3):
    best_score = 0.0 #symbolic high value :-)
    best_grid = None #inicializar la variable
    #este loop inicia las pruebas secuenciales de los pipelines:
    #es relevante que no sólo soporta 2, sino se va en cada uno 
    #de los que estén presentes en la lista
    for pipe in pipelines:
        try:
            #quiero que se desligue, para no modificar (al final, usa poco RAM)
            pipe = pipe.copy()
            #etapas del pipeline
            stages = pipe.getStages()
            #obtener el predictor (el motor ML)
            predictor = [stage for stage in stages
                if "pyspark.ml.classification" in str(type(stage)) or
                 "pyspark.ml.regression" in str(type(stage))][0]
            predictor_i = stages.index(predictor)
            stringer = [stage for stage in pipe.getStages()
                if "pyspark.ml.feature.StringIndexer" in str(type(stage))][0]
            if DEBUG: print("pipeline:\n%s\n\n"%stages)
            if DEBUG: print("predictor=%s (index %s, type %s), stringer=%s (%s)\n"%
                  (predictor, stages.index(predictor), type(predictor),
                  stringer, type(stringer)))
            #dado que no son predicciones susceptibles a cambios en el CV
            prepipe = Pipeline(stages=stages[0:(predictor_i)])
            if DEBUG: print("pre pipeline:\n%s\n\n"%prepipe.getStages())
            print("Starting fit on prepipe...")
            #modelo para el prepipeline
            prepipem = prepipe.fit(train)
            print("Starting transform on prepipe...")
            train2 = prepipem.transform(train)
            test2 = prepipem.transform(test)
            #el motor ML sí es susceptible CV
            postpipe = Pipeline(stages=stages[(predictor_i):len(stages)])
            if DEBUG: print("post pipeline:\n%s\n\n"%postpipe.getStages())
            #creación del Cross Validator
            gridcv = CrossValidator(estimator=postpipe,
                          estimatorParamMaps=grid,
                          evaluator=MulticlassClassificationEvaluator(labelCol="DEPARTURE_DELAY"),
                          numFolds=cvfolds)
            #extraemos el nombre y le quitamos la parte "fea" que devuelve type()
            predictr = [str(type(stage)) for stage in pipe.getStages() 
            if "pyspark.ml.classification" in str(type(stage)) or
             "pyspark.ml.regression" in str(type(stage))][0]
            predictr = re.sub("^<class 'pyspark\.ml\.(classification|regression)\.([a-zA-Z0-9]+)'>", 
                              "\\2", 
                              predictr)
            #creación del modelo
            print("Starting fit on %s..."%predictr)
            gridcvm = gridcv.fit(train2)
            #aplicación del modelo
            print("Starting transform on %s..."%predictr)
            preds = gridcvm.transform(test2)
            #obtenemos el evaluador para el uso en la medición de errores
            ev = gridcvm.getEvaluator()
            #obtenemos la métrica del error
            metric = ev.getMetricName()
            print("Starting error calculation on %s..."%predictr)
            #obtenemos el valor del eror
            error  = ev.evaluate(preds)
            print("Error %s: %f"% (metric, error))
            #si es mejor que el modelo pasado, lo guardamos. El último
            #guardado será el que devuelva esta función
            if error > best_score:
                print("%s is the best model so far: %f (%s)"%(predictr, error, metric))
                best_grid = gridcvm
        #manejo de errores y horrores
        except Exception as e:
            print('Error during Magic Loop:', e)
        continue
    return best_grid
#parametros para el magic loop
paramGrid = ParamGridBuilder() \
                .addGrid(glr.family, ["Gaussian", "Poisson", "Tweedie"]) \
                .addGrid(glr.maxIter, [1, 2, 3]) \
                .addGrid(lr.maxIter, [1, 2, 3]) \
                .addGrid(lr.elasticNetParam, [0.1,0.2,0.3]) \
                .build()
magic = magic_loop3(pipelines, paramGrid, train, test, 3)
#obtengo el pipeline que devolvió el magic loop
best_model = magic.getEstimator()
#obtenemos el paso del clasificador
best_estimator = best_model.getStages()[0]
#guardo en una variable los parámetros más adecuados
best_estimator_params = best_estimator.extractParamMap()
#obtener el evaluador para medir errores
ev0 = magic.getEvaluator()
###################
#medición del error
#impresión de los parámetros y el mejor modelo
print("%s (best model) Parameters: maxIter=%d, elasticNetParam=%f"%
      (re.sub("^<class 'pyspark\.ml\.(classification|regression)\.([a-zA-Z0-9]+)'>", 
                              "\\2", (str)((type(best_estimator)))), 
       best_estimator_params[best_estimator.maxIter], 
       best_estimator_params[best_estimator.elasticNetParam]))
#aquí probé como se veían los errores
preds = magic.transform(Pipeline(stages=pipeline1.getStages()[0:6]).fit(test).transform(test))
print("Error %s: %f"% (ev0.getMetricName(), ev0.evaluate(preds)))
