from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, IndexToString, VectorAssembler, VectorIndexer
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor, DecisionTreeRegressor, GeneralizedLinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline, Model
from timeit import default_timer as timer

spark = SparkSession.builder.getOrCreate()

flights = spark.read\
    .format('org.apache.spark.sql.execution.datasources.csv.CSVFileFormat')\
    .option('header', 'true')\
    .option('inferSchema', 'true')\
    .load('s3a://eduardomtz/flights/input_data/flights/flights.csv')

df_flights = flights.filter("CANCELLED = 0") \
    .select("DEPARTURE_DELAY",
            "MONTH", 
            "DAY",
            "DAY_OF_WEEK", 
            "AIRLINE",
            "ORIGIN_AIRPORT", 
            "DESTINATION_AIRPORT", 
            "TAXI_OUT", 
            "SCHEDULED_TIME",
            "ELAPSED_TIME", 
            "AIR_TIME", 
            "DISTANCE", 
            "TAXI_IN", 
            "ARRIVAL_DELAY",
            "DIVERTED")
            
df_flights = df_flights.na.drop(subset=["DEPARTURE_DELAY",
                                  "MONTH",
                                  "DAY",
                                  "DAY_OF_WEEK",
                                  "AIRLINE",
                                  "ORIGIN_AIRPORT", 
                                  "DESTINATION_AIRPORT", 
                                  "TAXI_OUT", 
                                  "SCHEDULED_TIME",
                                  "ELAPSED_TIME", 
                                  "AIR_TIME", 
                                  "DISTANCE", 
                                  "TAXI_IN", 
                                  "ARRIVAL_DELAY",
                                  "DIVERTED"])
                                  
df_flights = df_flights.select(col("DEPARTURE_DELAY").cast("integer").alias("label"),
                               col("MONTH").cast("integer"), 
                               col("DAY").cast("integer"),
                               col("DAY_OF_WEEK").cast("integer"), 
                               "AIRLINE",
                               "ORIGIN_AIRPORT", 
                               "DESTINATION_AIRPORT", 
                               col("TAXI_OUT").cast("integer"), 
                               col("SCHEDULED_TIME").cast("integer"), 
                               col("ELAPSED_TIME").cast("integer"), 
                               col("AIR_TIME").cast("integer"), 
                               col("DISTANCE").cast("integer"), 
                               col("TAXI_IN").cast("integer"), 
                               col("ARRIVAL_DELAY").cast("integer"),
                               col("DIVERTED").cast("integer"))

df_flights.show()

train_flights, test_flights = df_flights.randomSplit([0.7, 0.3])

print("Número de registros de entrenamiento: " + str(train_flights.count()))
print("Número de registros de prueba: " + str(test_flights.count()))

aerolin_indexada = StringIndexer(inputCol="AIRLINE", outputCol="AIRLINE_NUM").setHandleInvalid("skip")
origen_indexada = StringIndexer(inputCol="ORIGIN_AIRPORT", outputCol="ORIGIN_AIRPORT_NUM").setHandleInvalid("skip")
destino_indexada = StringIndexer(inputCol="DESTINATION_AIRPORT", outputCol="DESTINATION_AIRPORT_NUM").setHandleInvalid("skip")
vectorAssembler_features = VectorAssembler(inputCols=["MONTH",
                                                      "DAY",
                                                      "DAY_OF_WEEK",
                                                      "AIRLINE_NUM",
                                                      "ORIGIN_AIRPORT_NUM",
                                                      "DESTINATION_AIRPORT_NUM",
                                                      "TAXI_OUT",
                                                      "SCHEDULED_TIME",
                                                      "ELAPSED_TIME",
                                                      "AIR_TIME",
                                                      "DISTANCE",
                                                      "TAXI_IN", 
                                                      "ARRIVAL_DELAY",
                                                      "DIVERTED"],
                                            outputCol="features")


metod = {
    'gbt': GBTRegressor(labelCol="label", featuresCol="features", maxBins = 640),
    'mlg': GeneralizedLinearRegression(labelCol="label", featuresCol="features", family="gaussian", link="identity")
}

grid = {
    'gbt': ParamGridBuilder() \
        .addGrid(metod['gbt'].maxDepth, [2, 5, 7])\
        .addGrid(metod['gbt'].maxIter, [3, 5, 9])\
        .build(),
    
    'mlg': ParamGridBuilder() \
        .addGrid(metod['mlg'].regParam, [0.1, 0.3, 0.5])\
        .addGrid(metod['mlg'].maxIter, [3, 5, 9])\
        .build()
}


def magic_loop(metod_ejec, metod, grid, train):
    ## Parametros:
    ## metod_ejec: Lista de modelos a ejecutar
    ## metod: Diccionario de modelos que se consideraran
    ## grid: ParamGrid con los parametros de los modelos a considerar
    ## train: datos de entrenamiento
    
    ## Se inicializan variables
    mejor_score = 0
    mejor_modelo = ''
    
    print("Modelo\tMetrica_desemp")
    
    for index, mdl in enumerate([metod[x] for x in metod_ejec]):

        pipeline_gral = Pipeline(stages=[aerolin_indexada,                                      
                                       origen_indexada,
                                       destino_indexada,
                                       vectorAssembler_features,
                                       mdl])

        crossval = CrossValidator(estimator=pipeline_gral,
                                  estimatorParamMaps=grid[metod_ejec[index]],
                                  evaluator=RegressionEvaluator(),
                                  numFolds=10)       

        cvModel = crossval.fit(train)

        print("{}\t{}".format(metod_ejec[index], cvModel.avgMetrics[0]))
    
        ## Se condiciona a seleccionar entre los modelos el mejor entre los modelos probados:
        if (mejor_score == 0) or (cvModel.avgMetrics[0] < mejor_score):
            mejor_score = cvModel.avgMetrics[0]
            mejor_modelo = cvModel
            
    return mejor_modelo

## Modelos que se ejecutaran
metod_ejec=['mlg','gbt']

## Ejecucion de la funcion funcion Magic Loop
start = timer()
ejec_magicloop = magic_loop(metod_ejec, metod, grid, train_flights)
end = timer()

print("Tiempo de ejecucion en minutos:", (end - start)/60)

ejec_magicloop.avgMetrics[0]

predict = ejec_magicloop.transform(test_flights)
predict.select('features', 'prediction').show()

evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")

rmse = evaluator.evaluate(predict)
print("Error cuadrático medio sobre los datos de prueba = %g" % rmse)