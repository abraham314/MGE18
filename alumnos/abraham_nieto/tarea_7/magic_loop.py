%pyspark

import datetime
a=datetime.datetime.now()

from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName('cruise').getOrCreate() 
flights = spark.read.csv('s3n://tarea4/flights.csv',inferSchema=True,header=True)

flights=flights.na.fill(0) #imputamos los missings de las variables numércias
flights=flights.na.fill("?")#imputamos los missng de las variables categóricas

columnList = [item[0] for item in flights.dtypes if item[1].startswith('string')] 

#Hacemos la partición de datos 70% para entrenamiento y 30% para test.
train_data,test_data = flights.randomSplit([0.7,0.3])

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import PCA
from pyspark.ml.feature import ChiSqSelector
#Creamos la función indexers para transformar los features de tipo categóricos a numéricos
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(flights) for column in 
            list(set(columnList)) ]
            

from pyspark.ml import Pipeline
#Creamos un pipeline pindexers para que la función indexers tenga métodos fit transform.
pindexers = Pipeline(stages=indexers) 

#Hacemos VectorAssembler para poder generar la variable features que es un vector de los valores de cada columna
#en este caso eliminamos algunas columnas ya que no hace sentido ocuparlas tipo el año, y variables de horas.
assembler = VectorAssembler(
  inputCols=[#'YEAR',Thin
             'MONTH',
             'DAY',
             'DAY_OF_WEEK',
             #'AIRLINE',
            'AIRLINE_index',
             'FLIGHT_NUMBER',
            # 'TAIL_NUMBER',
            #'ORIGIN_AIRPORT',
            'ORIGIN_AIRPORT_index',
            #'DESTINATION_AIRPORT',
            'DESTINATION_AIRPORT_index',
            'SCHEDULED_DEPARTURE',
            #'DEPARTURE_TIME',
            'TAXI_OUT',
            #'WHEELS_OFF',
            #'SCHEDULED_TIME',
            'ELAPSED_TIME',
            'AIR_TIME',
            'DISTANCE',
            #'WHEELS_ON',
            'TAXI_IN',
            #'SCHEDULED_ARRIVAL',
            #'ARRIVAL_TIME',
            'ARRIVAL_DELAY',
            #'DIVERTED',
            #'CANCELLED',
            #'CANCELLATION_REASON',
            #'AIR_SYSTEM_DELAY',
            #'SECURITY_DELAY',
            #'AIRLINE_DELAY','LATE_AIRCRAFT_DELAY','WEATHER_DELAY'
  ],
    outputCol="features")




#Normalizamos los features
scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")

#aplicamos pca para reducir dimensionalidad
pca = PCA(k=7, inputCol="scaled_features", outputCol="pcaFeatures")

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder 
from pyspark.ml.evaluation import RegressionEvaluator 
from pyspark.ml.regression import LinearRegression,DecisionTreeRegressor,GeneralizedLinearRegression

#Hacemos un udf para generar un tipo diccionario de modelos y de Paramgridbuilders respectivamente y estos
#se iteren con sus respectivos paraétros.
def define_hyper_params():

    #Creamos un diccionario de los modelos
    modelo = {'dt': DecisionTreeRegressor(featuresCol="pcaFeatures",labelCol='DEPARTURE_DELAY'),
    'rf': RandomForestRegressor(featuresCol="pcaFeatures",labelCol="DEPARTURE_DELAY")}
    
    #Creamos una lista de paramgrids para tener la lista de prarámetros con los que se hará Cross validtion.
    #en este caso para dt arbol de decision y rf randomforest.
    search_space = [ParamGridBuilder().\
    
    addGrid(modelo['dt'].maxBins, [10,15,20]).\
    addGrid(modelo['dt'].maxDepth, [3,5,10]).\
    build()
    ,
    ParamGridBuilder().\
    addGrid(modelo['rf'].numTrees, [10,15,20]).\
    addGrid(modelo['rf'].maxDepth, [5,10,15]).\
    build()]

    return (modelo,search_space)


from pyspark.ml import Pipeline
def magic_loop(X_train,models_to_run=['dt','rf']):#entradas dataframe X_train y diccionario de modelos
    modelo,search_space=define_hyper_params() #usamos la función define_hyper_params para seleccionar 
    #los paramgrids definidos.
    best=[] #lista para guardar los mejores modelos de cada algoritmo
    metr=[] #lista para guardar las métricas de ls mejores mdelos de cada algoritmo
    params=[] #lista para guardar los mejores parámetros de cada algoritmo.
    for i in range(len(models_to_run)):#corremos para cada modelo sus respectivos parametros
        #generamos el pipeline de todos los transformers que declaramos
        pipeline = Pipeline(stages=[pindexers,assembler,scaler,pca,modelo[models_to_run[i]]]) 
        #hacemos el cross validation con la lista de paramétros del models_to_run[i]
        crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=search_space[i],#parametros del modelo i
                          evaluator=RegressionEvaluator(predictionCol='prediction', labelCol="DEPARTURE_DELAY", 
                               metricName='rmse'),#metrica de comparacion default es rmse
                          numFolds=10)#corremos con 10 partciones el cross validation
        cvModel = crossval.fit(X_train)#ajuste del modelo
        best_model=cvModel.bestModel#generamos el mejor modelo con los mejores parametros para el algoritmo i
        best.append(best_model) #guardamos el mejor modelo del algoritmo i en la lista vest
        metr.append(min(cvModel.avgMetrics)) #guardamos la metrica rmse en la lista metr del mejor modelo
        #lo mismo con los parametros
        params.append(search_space[i][cvModel.avgMetrics.index(min(cvModel.avgMetrics))])
        
        #imprimimos los parametros del mejor modelo del algoritmo i
        print('Mejor modelo de ',models_to_run[i],'fue:',search_space[i][cvModel.avgMetrics.index(min(cvModel.avgMetrics))]) 
    
    print(metr)
    
    #imprimimos el modelo ganador
    print('Mejor modelo fue:',params[metr.index(min(metr))]) 
    
    #El mejor modelo es el que tienen el menor rmse y eso regresa el programa.        
    return(best[metr.index(min(metr))])


#guardamos el mejor modelo lo guardamos en champ y tomamos el tiempo de ejecucion de magic loop
champ=magic_loop(train_data,models_to_run=['dt','rf'])  

b=datetime.datetime.now()
print('tiempo de proceso',b-a)

Results=champ.transform(test_data) 

Results.select("DEPARTURE_DELAY",'prediction').show()
    
    
    
