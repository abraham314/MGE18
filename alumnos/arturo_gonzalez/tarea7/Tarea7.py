from pyspark.ml.feature import PCA
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression,RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder 

#Leemos archivo
datos = spark.read.csv('s3a://al102964-bucket1/tarea4/ejercicio_b/flights.csv',header=True,inferSchema=True)

#Lista para seleccionar los campos de utilidad
campos = ['YEAR', 'MONTH', 'DAY', 'DAY_OF_WEEK', 'AIRLINE','FLIGHT_NUMBER','ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE','DEPARTURE_DELAY', 'SCHEDULED_TIME','DISTANCE','SCHEDULED_ARRIVAL']

datos = datos.select(campos).na.drop()

datos = datos.withColumnRenamed("DEPARTURE_DELAY", "label")

#Separamos en train y test
(training_data, test_data) = datos.randomSplit([0.7, 0.3])

#Definimos lista de variables categoricas
categorical = ['AIRLINE','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
steps = [StringIndexer(inputCol=column, outputCol=column+"_INDEXED",handleInvalid='skip').fit(training_data) for column in categorical]

#Hacemos el onehot encoding a las variables categoricas 
encodersteps = [OneHotEncoder(inputCol=column+"_INDEXED", outputCol=column+"_OH") for column in categorical]

steps += encodersteps

#Definimos lista de campos a meter dentro de columna de features
campos = ['YEAR','MONTH','DAY','DAY_OF_WEEK','AIRLINE_OH','FLIGHT_NUMBER','ORIGIN_AIRPORT_OH','DESTINATION_AIRPORT_OH','SCHEDULED_DEPARTURE','SCHEDULED_TIME','DISTANCE','SCHEDULED_ARRIVAL']
assembler = VectorAssembler(inputCols=campos,outputCol="features_1")
steps.append(assembler)

#Definimos PCA para reducir dimensionalidad
pca = PCA(k=10, inputCol="features_1", outputCol="features")
steps.append(pca)

#Definimos modelo de regresion lineal
lr = LinearRegression()

#Lo vamos a necesitar para aplicar el RandomForestRegressor()
stepsrf = list(steps)

#Agregamos a los pasos a ejecutar por el pipeline de regresion lineal
steps.append(lr)

#Construimos el pipeline
pipeline_lr = Pipeline(stages=steps)

#Construimos el paramGrid para probar los mejores parametros del algoritmo de regresion
paramGrid_lr = ParamGridBuilder() \
    .addGrid(lr.maxIter, [5,10,15])\
    .addGrid(lr.regParam, [0.0,0.5,1.0]).build()


#Construimos un crossvalidator en donde especificamos el objeto pipeline, parameGrid, el evaluador y el numFolds
crossval_lr = CrossValidator(estimator=pipeline_lr,
                          estimatorParamMaps=paramGrid_lr,
                          evaluator=RegressionEvaluator(),
                          numFolds=10)  # use 3+ folds in practice


#Creamos un objeto del tipo RandomForest Regressor
rf = RandomForestRegressor()

#Lo agregamos al conjunto de steps
stepsrf.append(rf)

pipeline_rf = Pipeline(stages=stepsrf)

paramGrid_rf = ParamGridBuilder() \
    .addGrid(rf.numTrees, [3,5,7])\
    .addGrid(rf.maxBins, [20,25,32]).build()

crossval_rf = CrossValidator(estimator=pipeline_rf,
                          estimatorParamMaps=paramGrid_rf,
                          evaluator=RegressionEvaluator(),
                          numFolds=10)  # use 3+ folds in practice

import time

def magic_loop(modelos, data):
    resultado_modelos = []
    for model in modelos:			    
	    resultado_modelos.append(model.fit(data))
    return resultado_modelos



tiempo_inicio = time.time()
modelos = magic_loop([crossval_lr,crossval_rf], training_data)
tiempo_fin = time.time()

#Tiempo de ejecucion es la resta del tiempo_fin - tiempo_inicio y dividimos entre 60 para obtener minutos
print(El tiempo de ejecucion fue: +str(tiempo_fin - tiempo_inicio/60))


#Obtenemos predicciones con el modelo de regresion lineal
prediction = modelos[0].transform(test_data)
prediction.select("label","prediction").show()

#Verificamos el rmse del modelo de regresion lineal 
evaluator = RegressionEvaluator(metricName="rmse", labelCol="label",
                                predictionCol="prediction")
rmse = evaluator.evaluate(prediction)
print("Root-mean-square error = " + str(rmse))


#Obtenemos predicciones con el modelo de random forest regressor
prediction = modelos[1].transform(test_data)
prediction.select("label","prediction").show()

#Evaluamos el modelo de random forest regressor
evaluator = RegressionEvaluator(metricName="rmse", labelCol="label",
                                predictionCol="prediction")
rmse = evaluator.evaluate(prediction)
print("Root-mean-square error = " + str(rmse))

'''
Referencias
https://spark.apache.org/docs/2.1.1/ml-pipeline.html
https://spark.apache.org/docs/2.1.1/ml-tuning.html
http://apache-spark-user-list.1001560.n3.nabble.com/StringIndexer-on-several-columns-in-a-DataFrame-with-Scala-td29842.html
https://spark.apache.org/docs/2.1.0/mllib-ensembles.html
Notas de clase de Liliana Millan 
'''