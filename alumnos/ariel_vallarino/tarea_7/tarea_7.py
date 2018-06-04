
# ## Tarea 7
# Ariel Vallarino - 175875


#-- Cargo librerias:
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from timeit import default_timer as timer

from pyspark.ml.feature import StringIndexer, IndexToString, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, DecisionTreeRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline, Model


#-- Defino SparkSession
spark = SparkSession.builder.getOrCreate()


# #### Cargo datos:
#-- Ruta del archivo en S3:
path_flights = 's3a://basharino/flights/flights/flights.csv'

#-- Cargo datos (con header):
df_data_orig = spark.read     .format('org.apache.spark.sql.execution.datasources.csv.CSVFileFormat')     .option('header', 'true')     .option('inferSchema', 'true')     .load(path_flights)


# #### Selecciono columnas de interes y elimino NA:
# DEPARTURE_DELAY es la variable a predecir. Se renombra como **label**


#-- Selecciono columnas de interes:
#-- DEPARTURE_DELAY es la variable a predecir. Se renombra como label
df_data = df_data_orig.filter("CANCELLED = 0")     .select("MONTH", 
            "DAY",
            "DAY_OF_WEEK", 
            "AIRLINE",
            "ORIGIN_AIRPORT", 
            "DESTINATION_AIRPORT", 
            "SCHEDULED_DEPARTURE", 
            "TAXI_OUT", 
            "SCHEDULED_TIME", 
            "ELAPSED_TIME", 
            "AIR_TIME", 
            "DISTANCE", 
            "TAXI_IN",
            "ARRIVAL_DELAY", 
            col("DEPARTURE_DELAY").alias("label")) 
# WEATHER_DELAY    


#-- Elimino registros con nulos:
df_data = df_data.na.drop(subset=["MONTH", 
                                  "DAY",
                                  "DAY_OF_WEEK",
                                  "AIRLINE",
                                  "ORIGIN_AIRPORT",
                                  "DESTINATION_AIRPORT",
                                  "SCHEDULED_DEPARTURE",
                                  "TAXI_OUT",
                                  "SCHEDULED_TIME",
                                  "ELAPSED_TIME",
                                  "AIR_TIME",
                                  "DISTANCE",
                                  "TAXI_IN",
                                  "ARRIVAL_DELAY",
                                  "label"])


#-- cantidad de registros:
df_data.count()


#-- Visualizo 1ros. registros:
df_data.show(10)


# Visualizo esquema:
df_data.printSchema()


# #### Separo dato en Entrenamiento y Prueba (70%, 30%)
#-- Separo datos en Entrenamiento y Prueba (70%, 30%)
train_data, test_data = df_data.randomSplit([0.7, 0.3])

print("Cant. de registros de Entrenamiento: " + str(train_data.count()))
print("Cant. de registros de Prueba       : " + str(test_data.count()))


#-- Convierto los campos que son de tipo String a numericos utilizando StringIndexer:
stringIndexer_airl = StringIndexer(inputCol="AIRLINE", outputCol="AIRLINE_IX").setHandleInvalid("skip")
stringIndexer_orig = StringIndexer(inputCol="ORIGIN_AIRPORT", outputCol="ORIGIN_AIRPORT_IX").setHandleInvalid("skip")
stringIndexer_dest = StringIndexer(inputCol="DESTINATION_AIRPORT", outputCol="DESTINATION_AIRPORT_IX").setHandleInvalid("skip")


# #### Creo Feature Vector:
#-- Creo Feature Vector:
vectorAssembler_features = VectorAssembler(inputCols=["MONTH",
                                                      "DAY",
                                                      "DAY_OF_WEEK",
                                                      "AIRLINE_IX",
                                                      "ORIGIN_AIRPORT_IX",
                                                      "DESTINATION_AIRPORT_IX",
                                                      "SCHEDULED_DEPARTURE",
                                                      "TAXI_OUT",
                                                      "SCHEDULED_TIME",
                                                      "ELAPSED_TIME",
                                                      "AIR_TIME",
                                                      "DISTANCE",
                                                      "TAXI_IN",
                                                      "ARRIVAL_DELAY"], 
                                            outputCol='features')


# #### Crea diccionarios con los parametros de los modelos a probar:
#-- Crea diccionarios con los parametros de los modelos a probar:

#-- Regressors --:
rgrs = {
    'RandomForest': RandomForestRegressor(featuresCol='features', labelCol='label', maxBins=650),
    'DecisionTree': DecisionTreeRegressor(featuresCol='features', labelCol='label', maxBins=650), 
    'GradientBoosted': GBTRegressor(featuresCol='features', labelCol='label', maxBins=650),
    'LinearRegression': LinearRegression(featuresCol='features', labelCol='label'),
}

#-- Parámetros --:
grid = {
    'RandomForest': ParamGridBuilder() \
        .addGrid(rgrs['RandomForest'].numTrees, [3, 5, 10]) \
        .addGrid(rgrs['RandomForest'].maxDepth, [2, 3, 5]) \
        .build(),
    
    'DecisionTree': ParamGridBuilder() \
        .addGrid(rgrs['DecisionTree'].minInfoGain, [0.0, 0.3, 0.5]) \
        .addGrid(rgrs['DecisionTree'].maxDepth, [2, 3, 5]) \
        .build(),

    'LinearRegression': ParamGridBuilder() \
        .addGrid(rgrs['LinearRegression'].regParam, [0.2, 0.5, 0.8]) \
        .addGrid(rgrs['LinearRegression'].maxIter, [2, 3, 5]) \
        .build(),

    'GradientBoosted': ParamGridBuilder() \
        .addGrid(rgrs['GradientBoosted'].maxDepth, [3, 5, 8]) \
        .addGrid(rgrs['GradientBoosted'].maxIter, [2, 3, 5]) \
        .build(), 
}       

#-- Modelos a correr --:
models_to_run=['DecisionTree','RandomForest'] 


# #### Funcion: MAGIC LOOP

#-- Funcion: MAGIC LOOP:
def magic_loop(models_to_run, regrs, grid, train, stIndexer_airl, stIndexer_orig, stIndexer_dest, vectorAssembler):
    # Parametros:
    ## models_to_run: Lista de modelos a ejecutar
    ## regrs: Modelos
    ## grid: ParamGrid con los parametros de los modelos
    ## train: datos de entrenamiento
    ## stIndexer_airl, stIndexer_orig, stIndexer_dest: indexer de columnas String
    ## --- Se deberia cambiar a una lista para que sea mas dinamico.
    ## vectorAssembler: features
    
    best_score = 0
    best_model = ''
    
    print("Modelo\tDesempeño")
    
    #-- Reccorro lista de modelos a correr: 
    for index, rgr in enumerate([rgrs[x] for x in models_to_run]):

        #-- Defino Pipeline:     
        pipeline_ml = Pipeline(stages=[stIndexer_airl,                                      
                                       stIndexer_orig,
                                       stIndexer_dest,
                                       vectorAssembler,
                                       rgr])
        
        #-- Evaluator
        # evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
        
        #-- Cross Validation:
        crossval = CrossValidator(estimator=pipeline_ml,
                                  estimatorParamMaps=grid[models_to_run[index]],
                                  evaluator=RegressionEvaluator(),  # metricName por defecto: "rmse"
                                  numFolds=10)       

        #-- Cross validation tambien es un estimador, le hacemos fit
        cvModel = crossval.fit(train)

        #-- Imprimo info. para analizar ejecuciones:
        print("{}\t{}".format(models_to_run[index], cvModel.avgMetrics[0]))
    
        #-- El Cross-Validation con el ParamGrid retorna el mejor modelo 
        ## luego de ejecutar toda las combinaciones de hiperparametros
        #-- Valido el error obtenido para retornar el mejor entre los modelos probados:
        if (best_score == 0) or (cvModel.avgMetrics[0] < best_score):
            best_score = cvModel.avgMetrics[0]
            best_model = cvModel
            
    return best_model


# Ejecuto funcion Magic Loop
start = timer()
modelo_mloop = magic_loop(models_to_run, rgrs, grid, train_data, stringIndexer_airl, stringIndexer_orig,  stringIndexer_dest, vectorAssembler_features)
end = timer()  


print("Tiempo de ejecucion:", round((end - start)/60), "minutos")

modelo_mloop.avgMetrics[0]


modelo_mloop.bestModel.stages[-1].extractParamMap()

predict = modelo_mloop.transform(test_data)

predict.select('features', 'prediction').show()

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")

rmse = evaluator.evaluate(predict)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

