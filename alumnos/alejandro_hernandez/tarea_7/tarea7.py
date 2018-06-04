%pyspark
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline
from timeit import default_timer as timer

sqlContext = SQLContext(sc)

# Leo la base flights.csv
vuelos = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3a://aws-alex-03032018-metodos-gran-escala/datos/flights.csv')

# Elimino aquellas variables que tienen datos "NA", con excepción de "DEPARTURE_DELAY" pues es la variable objetivo, por lo que en este caso pongo en ceros los registros para esta variable que tienen "NA".
# Asimismo, selecciono las variables que sulten de interés para predecir la variable "DEPARTURE_DELAY" sin incrementar la dimensionalidad.
vuelos_selec =vuelos.select('MONTH','DAY','DAY_OF_WEEK','FLIGHT_NUMBER','AIR_TIME','DISTANCE','DIVERTED','CANCELLED','DEPARTURE_DELAY').na.drop(subset='DEPARTURE_DELAY')
vuelos_selec = vuelos_selec.na.fill(0)

# Hago el VectorAssembler para definir los features.
vec_ass = VectorAssembler(inputCols=['MONTH','DAY','DAY_OF_WEEK','FLIGHT_NUMBER','AIR_TIME','DISTANCE','DIVERTED','CANCELLED'],outputCol='features')

# Estandarizo las variables.
std_scaler = StandardScaler(inputCol='features', outputCol='scaled_features')

# Defino un "Decision tree regression" que reciba los features ya estandarizados.
dtr = DecisionTreeRegressor(featuresCol='scaled_features',labelCol='DEPARTURE_DELAY')

# Defino un "Random forest regression" que reciba los features ya estandarizados.
rfr = RandomForestRegressor(featuresCol='scaled_features',labelCol='DEPARTURE_DELAY')

# Defino los pipelines de los dos modelos a utilizar.
pipeline = {}
paramGrid = {}
pipeline['dtr'] = Pipeline(stages=[vec_ass,std_scaler,dtr])
pipeline['rfr'] = Pipeline(stages=[vec_ass,std_scaler,rfr])

# Defino los Grid sobre el cual van a probar parámetros para ambos modelos.
paramGrid['dtr'] = ParamGridBuilder() \
    .addGrid(dtr.maxBins, [5, 10, 15]) \
    .addGrid(dtr.minInstancesPerNode, [2, 5, 10]) \
    .build()

paramGrid['rfr'] = ParamGridBuilder() \
    .addGrid(rfr.maxDepth, [2, 5, 7]) \
    .addGrid(rfr.numTrees, [5, 8, 10]) \
    .build()


# Defino el evaluador con la metrica rmse.
eval = RegressionEvaluator(metricName='rmse', predictionCol='prediction', labelCol="DEPARTURE_DELAY")

# Divido la base en 70% entrenamiento y 30% prueba.
(entrena,prueba) = vuelos_selec.randomSplit([0.7,0.3])

start = timer()

for mod in pipeline:
    # Realizo vallidación cruzada utilizando 10 folds como se indica en las notas.
    crossval = CrossValidator(estimator=pipeline[mod],
                          estimatorParamMaps=paramGrid[mod],
                          evaluator=eval,
                          numFolds=10)
    # Entreno el modelo con los datos de entrenamiento.
    cv_modelo = crossval.fit(entrena)

    # Hago las predicciones.
    predicciones = cv_modelo.transform(prueba)

    # Imprimo el modelo, el mejor modelo y el rmse.
    print("Modelo: " + mod)
    mejor_pipeline = cv_modelo.bestModel
    mejor_modelo = mejor_pipeline.stages[2]
    print("El mejor modelo es es: ",mejor_modelo)
    print("------------")
    print("Parametros:")
    print(mejor_modelo.extractParamMap())
    rmse = eval.evaluate(predicciones)
    print("------------")
    print("El rmse es: " ,rmse)
    print("------------")
    print("------------")
    print("------------")

end = timer()
print("Tiempo de ejecución del Magic Loop:", end - start)
