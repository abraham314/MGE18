%spark.pyspark
# Importamos todo lo que necesitamos
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql.functions import col 
from pyspark.ml.feature import VectorAssembler, VectorIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

# Objeto donde se almacenarán los pipelines para el Magic Loop
pipeline = {}

# Importamos la tabla "flights" que cargué en s3 y se imprimen los nombres de sus columnas para ver que todo bien.
sqlContext = SQLContext(sc)
flights = sqlContext.read.load('s3a://tarea7/flights.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
flights.cache()


# Se seleccionan variables que podrían ser relevantes y la variable objetivo "DEPARTURE_DELAY", Se limpian las columnas que tienen valores en null y Se castea todo como 'double'
df = df.select([col(c).cast("double").alias(c) for c in flights.select('YEAR', 'MONTH', 'DAY', 'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'DEPARTURE_DELAY').where(col("DEPARTURE_TIME").isNotNull() & col("DEPARTURE_DELAY").isNotNull()).columns])

# Se divide todo en train (70%) y test (30%)
train, test = df.randomSplit([0.7, 0.3])
print ("Hay %d observaciones de entrenamiento y %d de prueba." % (train.count(), test.count()))


featuresCols = df.columns
# Quitamos la objetivo
featuresCols.remove('DEPARTURE_DELAY')

# Hacemos el vector assembler  que ponen todas las variables independientes (todas menos la objetivo) en una columna
vectorAssembler = VectorAssembler(inputCols=featuresCols, outputCol="rawFeatures")

# Y el indexer que intenta adivinar cuales son categóricas.
vectorIndexer = VectorIndexer(inputCol="rawFeatures", outputCol="features", maxCategories=4)

# Ahora ajustamos GBTRegressor para que aprenda a predecir "DEPARTURE_DELAY"
gbt = GBTRegressor(labelCol="DEPARTURE_DELAY")

# Definimos el Grid de parámetros con tres valores diferentes para la profundidad maxima de cada arbol (maxDepth) y para el número de arboles en cada iteración (maxIter)
paramGrid = ParamGridBuilder()\
  .addGrid(gbt.maxDepth, [5, 10, 20])\
  .addGrid(gbt.maxIter, [5, 10, 20])\
  .build()


# Definimos la métrica de evaluación, en este caso es 'rmse'. el objeto gbt ya sabe cual es su 'label col' y su 'prediction col'
evaluator = RegressionEvaluator(metricName="rmse", labelCol=gbt.getLabelCol(), predictionCol=gbt.getPredictionCol())

# Declaramos el 'CrossValidator' que hará el tuneo del modelo, le especificamos el estimador (bgt), el evaluador y el grid de parámetros (todo lo definido anteriormente).
cv = CrossValidator(estimator=gbt, evaluator=evaluator, estimatorParamMaps=paramGrid)


# Finalmente definimos el pipeline y los "stages" que recibe son el vectorAssembler, vectorIndexer y el CrossValidator.
pipeline['GBT'] = Pipeline(stages=[vectorAssembler, vectorIndexer, cv])

# Ya tenemos el pipeline del primer algoritmo, ahora se agregará otro con RandomForest:


# Ahora ajustamos RandomForestRegressor para que aprenda a predecir "DEPARTURE_DELAY"
rfr = RandomForestRegressor(labelCol="DEPARTURE_DELAY")

# Definimos el Grid de parámetros con tres valores diferentes para la profundidad maxima de cada arbol (maxDepth) y para el número de arboles en cada iteración (maxIter)
paramGrid = ParamGridBuilder()\
  .addGrid(rfr.maxDepth, [3,5,7])\
  .addGrid(rfr.numTrees, [5, 10, 30])\
  .build()
# Definimos la métrica de evaluación, en este caso es 'rmse'. el objeto rfr ya sabe cual es su 'label col' y su 'prediction col'
evaluator = RegressionEvaluator(metricName="rmse", labelCol=rfr.getLabelCol(), predictionCol=rfr.getPredictionCol())
# Declaramos el 'CrossValidator' que hará el tuneo del modelo, le especificamos el estimador (bgt), el evaluador y el grid de parámetros (todo lo definido anteriormente).
cv = CrossValidator(estimator=rfr, evaluator=evaluator, estimatorParamMaps=paramGrid)


pipeline['RFR'] = Pipeline(stages=[vectorAssembler, vectorIndexer, cv])

# Magic Loop
# Aquí sucede el Magic Loop que hace el fit y el transform de los datos e imprime el RMSE de las predicciones del mejor modelo encontrado.
# Este codigo tarda más de 6 horas en correr (ojo).
for pipl in pipeline:
	pipelineModel = pipl.fit(train)
	predictions = pipelineModel.transform(test)
	rmse = evaluator.evaluate(predictions)
	print ("RMSE para la mejor versión del algoritmo:" + pipl.key + " fue de: " + rmse)








